(ns duratom.utils
  (:require [clojure.java.io :as jio]
            [clojure.edn :as edn]
            [clojure.java.jdbc :as sql])
  (:import (java.io PushbackReader)
           (java.nio.file StandardCopyOption Files)
           (java.util.concurrent.locks Lock)
           (java.util.concurrent.atomic AtomicBoolean)))

(defn read-edn-from-file!
  "Efficiently read large data structures from a stream."
  [filepath]
  (with-open [r (PushbackReader. (jio/reader filepath))]
    (edn/read r)))

(defn write-edn-to-file!
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

(defn delete-dedicated-table! [config table-name]
  (sql/db-do-commands config (sql/drop-table-ddl table-name)))

(defn create-dedicated-table! [db-config table-name]
  (sql/db-do-commands db-config (sql/create-table-ddl table-name [[:id :int] [:value :text]])))

(defn get-value [db table-name]
  (sql/query db [(str "SELECT value FROM " table-name " LIMIT 1")]
             {:row-fn (comp edn/read-string :value)
              :resultset-fn first}))

(defn table-exists? [db table-name]
  (sql/query db ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"]
             {:resultset-fn #(some #{table-name} (map :table_name %))}))
