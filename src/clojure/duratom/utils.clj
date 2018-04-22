(ns duratom.utils
  (:require [clojure.java.io :as jio]
            [clojure.edn :as edn])
  (:import (java.io PushbackReader BufferedWriter)
           (java.nio.file StandardCopyOption Files)
           (java.util.concurrent.locks Lock)
           (java.util.concurrent.atomic AtomicBoolean)
           (java.sql BatchUpdateException)
           (dbaos DirectByteArrayOutputStream)
           (java.security MessageDigest)))

(try
  (require '[clojure.java.jdbc :as sql])
  (catch Exception e
    (require '[duratom.not-found.jdbc :as sql])))

(try
  (require '[amazonica.aws.s3 :as aws])
  (catch Exception e
    (require '[duratom.not-found.s3 :as aws])))

(defn pr-str-fully
  "Wrapper around `pr-str` which binds *print-length* to nil."
  ^String [& xs]
  (binding [*print-length* nil]
    (apply pr-str xs)))

(defn s3-bucket-bytes
  "A helper for pulling out the bytes out of an S3 bucket,
   which has the potential for zero copying."
  (^bytes [s3-in]
   (s3-bucket-bytes 4096 s3-in))
  (^bytes [buffer-size s3-in]
   (with-open [in (jio/input-stream s3-in)]
     (let [bs (int buffer-size)
           out (DirectByteArrayOutputStream. bs)] ;; allows for copy-less streaming when size is known
       (jio/copy in out :buffer-size bs) ;; minimize number of loops required to fill the `out` buffer
       (.toByteArray out)))))

(defn read-edn-object
  "Efficiently read one data structure from a stream.
   Both arities do the same thing."
  ([source]
   (read-edn-object nil source))
  ([_buffer source]
   (with-open [r (PushbackReader. (jio/reader source))]
     (edn/read r))))

(defn write-edn-object
  "Efficiently write large data structures to a stream."
  [filepath data]
  (with-open [^BufferedWriter w (jio/writer filepath)]
    (.write w (pr-str-fully data))))

(defn read-edn-objects
  "Efficiently multiple data structures from a stream."
  [source]
  (let [eof (Object.)]
    (with-open [rdr (PushbackReader. (jio/reader source))]
      (->> (partial edn/read {:eof eof} rdr)
           repeatedly
           (take-while (partial not= eof))
           doall))))

(def move-opts
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
     (throw (IllegalStateException. "Duratom has been released!"))))

(defn- md5sum
  "Calculates the MD5 checksum for given bytes <xs>."
  ^String [^bytes xs]
  (let [hsh (-> (MessageDigest/getInstance "MD5")
                (.digest xs))]
    (format "%032x" (BigInteger. 1 hsh)))) ;; md5 is fixed size (32)

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

(defn table-exists? [db table-name]
  (sql/query db ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"]
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
