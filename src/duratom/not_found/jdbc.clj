(ns duratom.not-found.jdbc)

(defn update-or-insert!
  [db table row where-clause]
  (throw (UnsupportedOperationException.
           "DB backend requires that you have `clojure.java.jdbc` on your classpath...")))

(defn delete-relevant-row! [config table-name row-id]
  (throw (UnsupportedOperationException.
           "DB backend requires that you have `clojure.java.jdbc` on your classpath...")))

(defn create-dedicated-table! [db-config table-name]
  (throw (UnsupportedOperationException.
           "DB backend requires that you have `clojure.java.jdbc` on your classpath...")))

(defn get-pgsql-value [db table-name row-id]
  (throw (UnsupportedOperationException.
           "DB backend requires that you have `clojure.java.jdbc` on your classpath...")))

(defn table-exists? [db table-name]
  (throw (UnsupportedOperationException.
           "DB backend requires that you have `clojure.java.jdbc` on your classpath...")))
