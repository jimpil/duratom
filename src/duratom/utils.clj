(ns duratom.utils
  (:require [clojure.java.io :as jio]
            [clojure.edn :as edn])
  (:import (java.io PushbackReader)
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

(defn read-edn!
  "Efficiently read large data structures from a stream."
  [source]
  (with-open [r (PushbackReader. (jio/reader source))]
    (edn/read r)))

(defn write-edn!
  "Efficiently write large data structures to a stream."
  [data filepath]
  (with-open [w (jio/writer filepath)]
    (binding [*out* w]
      (pr data))))

(defn move-file!
  [source target]
  (Files/move (.toPath (jio/file source))
              (.toPath (jio/file target))
              (into-array [StandardCopyOption/ATOMIC_MOVE
                           StandardCopyOption/REPLACE_EXISTING])))

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

(defmacro assert-not-released! [release-fn]
  `(assert (not (~release-fn)) "Duratom has been released!"))

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

(defn create-dedicated-table! [db-config table-name]
  (try
    (sql/db-do-commands db-config (sql/create-table-ddl table-name [[:id :int] [:value :text]]))
    (catch BatchUpdateException _ '(0)))) ;; table already exists!

(defn get-pgsql-value [db table-name row-id]
  (sql/query db [(str "SELECT value FROM " table-name " WHERE id = " row-id " LIMIT 1")]
             {:row-fn (comp edn/read-string :value)
              :result-set-fn first}))

(defn table-exists? [db table-name]
  (sql/query db ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"]
             {:row-fn :table_name
              :result-set-fn #(some? (some #{table-name} %))}))

;; S3 utils

(defn create-s3-bucket [creds bucket-name]
  (aws/create-bucket creds bucket-name))

(defn get-value-from-s3 [creds bucket-name key]
  (-> (aws/get-object creds bucket-name key)
      :input-stream
      read-edn!))

(defn store-value-to-s3 [creds bucket key value]
  (let [str-val (pr-str value)
        str-val-bytes (.getBytes str-val)]
    (aws/put-object creds bucket key (jio/input-stream str-val-bytes) {:content-length (.length str-val)})))

(defn delete-object-from-s3 [credentials bucket-name k]
  (aws/delete-object credentials bucket-name k))

(defn does-bucket-exist [creds bucket-name]
  (aws/does-bucket-exist creds bucket-name))
