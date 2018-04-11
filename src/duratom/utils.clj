(ns duratom.utils
  (:require [clojure.java.io :as jio]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import (java.io PushbackReader BufferedWriter InputStream ByteArrayOutputStream)
           (java.nio.file StandardCopyOption Files)
           (java.util.concurrent.locks Lock)
           (java.util.concurrent.atomic AtomicBoolean)
           (java.sql BatchUpdateException)))

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
  "A helper for pulling out the bytes out of an S3 bucket."
  (^bytes [s3-in]
   (s3-bucket-bytes 1024 s3-in))
  (^bytes [buffer-size ^InputStream s3-in]
   (with-open [in (io/input-stream s3-in)]
     (let [out (ByteArrayOutputStream. (int buffer-size))]
       (io/copy in out)
       (.toByteArray out)))))

(defn read-edn-from-file!
  "Efficiently read large data structures from a stream."
  [source]
  (with-open [r (PushbackReader. (jio/reader source))]
    (edn/read r)))

(defn write-edn-to-file!
  "Efficiently write large data structures to a stream."
  [filepath data]
  (with-open [^BufferedWriter w (jio/writer filepath)]
    (.write w (pr-str-fully data))))

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
              :result-set-fn #(some? (some #{table-name} %))}))

;; S3 utils

(defn create-s3-bucket [creds bucket-name]
  (aws/create-bucket creds bucket-name))

(defn get-value-from-s3 [creds bucket-name key read-it!]
  (-> (aws/get-object creds bucket-name key)
      :input-stream
      read-it!))

(defn store-value-to-s3 [creds bucket key value]
  (let [^bytes val-bytes (if (string? value)
                           (.getBytes ^String value)
                           value)]
    (aws/put-object creds bucket key
                    (jio/input-stream val-bytes)
                    {:content-length (alength val-bytes)})))

(defn delete-object-from-s3 [credentials bucket-name k]
  (aws/delete-object credentials bucket-name k))

(defn bucket-exists? [creds bucket-name]
  (aws/does-bucket-exist creds bucket-name))
