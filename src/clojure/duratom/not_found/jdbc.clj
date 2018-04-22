(ns duratom.not-found.jdbc)

(defmacro with-db-transaction [bs & body]
  `(let ~bs
     (throw (UnsupportedOperationException.
              "DB backend requires that you have `clojure.java.jdbc` on your classpath..."))))

(defn update!
  [db table row where-clause]
  (throw (UnsupportedOperationException.
           "DB backend requires that you have `clojure.java.jdbc` on your classpath...")))

(defn insert!
  [db table row where-clause]
  (throw (UnsupportedOperationException.
           "DB backend requires that you have `clojure.java.jdbc` on your classpath...")))

(defn db-do-commands [config & commands]
  (throw (UnsupportedOperationException.
           "DB backend requires that you have `clojure.java.jdbc` on your classpath...")))

(defn create-table-ddl [db-config table-name]
  (throw (UnsupportedOperationException.
           "DB backend requires that you have `clojure.java.jdbc` on your classpath...")))

(defn query [db table-name row-id]
  (throw (UnsupportedOperationException.
           "DB backend requires that you have `clojure.java.jdbc` on your classpath...")))

