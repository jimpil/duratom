(ns duratom.utils
  (:refer-clojure :exclude [identity])
  (:require [clojure.java.io :as jio]
            [clojure.edn :as edn]
            [duratom.readers :as readers])
  (:import (java.io PushbackReader BufferedWriter ByteArrayOutputStream)
           (java.nio.file StandardCopyOption Files)
           (java.util.concurrent.locks Lock)
           (java.util.concurrent.atomic AtomicBoolean)
           (java.sql BatchUpdateException)
           (dbaos DirectByteArrayOutputStream)
           (java.security MessageDigest)
           (java.util Base64)
           (duratom.readers ObjectWithMeta)
           (java.net URL)))

(try
  (require '[clojure.java.jdbc :as sql])
  (catch Exception e
    (require '[duratom.not-found.jdbc :as sql])))

(try
  (require '[amazonica.aws.s3 :as aws])
  (catch Exception e
    (require '[duratom.not-found.s3 :as aws])))

(try
  (require '[taoensso.carmine :as car])
  (catch Exception e
    (require '[duratom.not-found.redis :as car])))

(defn iobj->edn-tag
  "Helper fn for constructing ObjectWithMeta wrapper.
   An object of this type will essentially be serialised
   as a vector of two elements - the <coll> and its metadata map.
   It will be read back as <coll> with the right metadata attached."
  [coll]
  (if (some? (meta coll))
    (ObjectWithMeta. coll)
    coll))

(defn pr-str-fully
  "Wrapper around `pr-str` which binds
   *print-length*, *print-length* & *print-meta* to nil."
  ^String [unpack-meta? & xs]
  (binding [*print-length* nil
            *print-level*  nil
            *print-meta*   nil
            *print-dup*    nil]
    (cond->> xs
             unpack-meta? (map iobj->edn-tag)
             true (apply pr-str))))

(defn s3-bucket-bytes
  "A helper for pulling out the bytes out of an S3 bucket,
   which has the potential for zero copying."
  (^bytes [s3-in]
   (s3-bucket-bytes 4096 s3-in))
  (^bytes [buffer-size s3-in]
   (with-open [in (jio/input-stream s3-in)
               out (DirectByteArrayOutputStream. (int buffer-size))] ;; allows for copy-less streaming when size is known
     (jio/copy in out :buffer-size buffer-size) ;; minimize number of loops required to fill the `out` buffer
     (.toByteArray out))))

(defn read-edn-object
  "Efficiently read one data structure from a stream."
  ([source]
   (read-edn-object readers/default source))
  ([opts source]
   (read-edn-object opts nil source))
  ([opts _ source] ;; need this arity
   (with-open [r (PushbackReader. (jio/reader source))]
     (edn/read opts r))))

(defn read-edn-string
  "Efficiently read one data structure from a stream."
  ([s]
   (read-edn-string readers/default s))
  ([opts s]
   (edn/read-string opts s)))

(defn write-edn-object
  "Efficiently write large data structures to a stream."
  ([filepath data]
   (write-edn-object true filepath data))
  ([unpack-meta? filepath data]
   (with-open [^BufferedWriter w (jio/writer filepath)]
     (->> data
          (pr-str-fully unpack-meta?)
          (.write w)))))

(defn read-edn-objects ;; not used anywhere - remove???
  "Efficiently multiple data structures from a stream."
  [opts source]
  (let [eof (Object.)]
    (with-open [rdr (PushbackReader. (jio/reader source))]
      (->> rdr
           (partial edn/read (merge readers/default opts {:eof eof}))
           repeatedly
           (take-while (partial not= eof))
           doall))))

(defonce move-opts
  (into-array [StandardCopyOption/ATOMIC_MOVE
               StandardCopyOption/REPLACE_EXISTING]))

(defn move-file!
  [source target]
  (Files/move (.toPath (jio/file source))
              (.toPath (jio/file target))
              move-opts))

(defn releaser []
  (let [released? (AtomicBoolean. false)]
    (fn release
      ([]
       (.get released?))
      ([value]
       (.set released? value)
       value))))

(defmacro with-locking
  "Like `locking`, but expects a `java.util.concurrent.locks.Lock` <lock>."
  [lock & body]
  `(try
     (.lock ~lock)
     ~@body
     (finally (.unlock ~lock))))

(defmacro lock? [l]
  `(instance? Lock ~l))

(defmacro assert-not-released!
  [release-fn]
  `(when (~release-fn)
     (throw (IllegalStateException. "duratom/duragent has been released!"))))

(defn- md5sum
  "Calculates the MD5 checksum for given bytes <xs>,
  and returns it in base64."
  ^String [^bytes xs]
  (let [hsh (-> (MessageDigest/getInstance "MD5")
                (.digest xs))]
    (.encodeToString (Base64/getEncoder) hsh)))

(defn ->init
  [x]
  (if (fn? x)
    (x)
    (force x))) ;; `force` returns x if not a delay

(defonce noop (constantly nil))
(defn identity [x & _] x)
(defonce FILE-IO-URL "https://file.io/")

(defn fileIO-get!
  "Returns the file specified by key <k> on file.io."
  ^bytes [k]
  (when (some? k)
    (let [urlk (URL. (str FILE-IO-URL k))
          baos (ByteArrayOutputStream.)]
      (with-open [in  (jio/input-stream urlk)
                  out baos]
        (jio/copy in out)
        (.toByteArray out)))))

(defn- fileIO-key
  [^String response]
  (some->> response
           (re-seq #"(\"key\":)\"(\w*)\"")
           first
           peek))

(defn fileIO-post!
  [http-post! data expiry]
  (when (some? data)
    (let [resp (http-post! (str FILE-IO-URL "?expires=" expiry) data)]
      (if (= 200 (:status resp))
        (fileIO-key (:body resp))
        (throw
          (IllegalStateException. "HTTP POST failed! Aborting..."))))))

;;===============<DB-UTILS>=====================================
(defn update-or-insert!
  "Updates columns or inserts a new row in the specified table."
  [db table row where-clause]
  (sql/with-db-transaction [t-conn db]
    (let [result (sql/update! t-conn table row where-clause)]
      (if (zero? (first result))
        (sql/insert! t-conn table row)
        result))))

(defn delete-relevant-row! [config table-name row-id]
  (try
    (sql/db-do-commands config (format "DELETE FROM %s WHERE id = %s" table-name row-id))
    (catch BatchUpdateException _ '(0)))) ;; table doesn't exist!

(defn create-dedicated-table! [db-config table-name col-type]
  (assert (some? col-type) "<col-type> is required!")
  (try
    (sql/db-do-commands db-config (sql/create-table-ddl table-name
                                                        [[:id :int] [:value col-type]]))
    (catch BatchUpdateException _ '(0)))) ;; table already exists!

(defn get-pgsql-value [db table-name row-id read-it!]
  (sql/query db [(str "SELECT value FROM " table-name " WHERE id = " row-id " LIMIT 1")]
             {:row-fn (comp read-it! :value)
              :result-set-fn first}))

(def get-sqlite-value get-pgsql-value)

(defn postgres-table-exists? [db table-name]
  (sql/query db ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"]
             {:row-fn :table_name
              :result-set-fn (comp some? (partial some #{table-name}))}))

(defn sqlite-table-exists? [db table-name]
  (sql/query db ["SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"]
             {:row-fn :table_name
              :result-set-fn (comp some? (partial some #{table-name}))}))

;; S3 utils

(defn create-s3-bucket [creds bucket-name]
  (aws/create-bucket creds bucket-name))

(defn get-value-from-s3 [creds bucket-name k metadata read-it!]
  (let [obj-size (when-not (-> read-it! meta :ignore-size?)
                   ;; make sure the reader needs the object-size, otherwise don't bother
                   (:content-length (aws/get-object-metadata creds bucket-name k)))]
    (->> (aws/get-object creds
                         :bucket bucket-name
                         :key k
                         :metadata metadata)
         :input-stream
         (read-it! obj-size))))

(defn store-value-to-s3 [creds bucket k value]
  (let [^bytes val-bytes (if (string? value)
                           (.getBytes ^String value)
                           value)]
    (aws/put-object creds
      :bucket-name bucket
      :key k
      :input-stream (jio/input-stream val-bytes)
      :metadata {:content-length (alength val-bytes)
                 :content-md5 (md5sum val-bytes)})))

(defn delete-object-from-s3 [credentials bucket-name k]
  (aws/delete-object credentials bucket-name k))

(defn bucket-exists? [creds bucket-name]
  (aws/does-bucket-exist creds bucket-name))

;;===============<REDIS-UTILS>=====================================

(defn redis-get [db-config key-name]
  (car/wcar db-config (car/get key-name)))

(defn redis-set [db-config key-name value]
  (car/wcar db-config (car/set key-name value)))

(defn redis-del [db-config key-name]
  (car/wcar db-config (car/del key-name)))

(defn redis-key-exists? [db-config key-name]
  (= 1 (car/wcar db-config (car/exists key-name))))
