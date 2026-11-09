(ns duratom.backends
  "Durable-storage backends.

  A backend is any value implementing `StorageBackend`. The file backend
  serializes to a local file; the SQL backend persists to sqlite or postgres
  through the jolt-lang/db library (the published clojure.jdbc on a java.sql
  shim). Add a new backend by implementing the protocol — duratom.core builds on
  any backend, not a closed set."
  (:require [duratom.utils :as ut]
            [jolt.fs :as fs]
            [db.jdbc]        ;; registers the java.sql shim over db.sqlite/db.pg
            [jdbc.core :as jdbc]))

(defprotocol StorageBackend
  (load-state [backend]     "Read and return the persisted value, or nil when none exists.")
  (persist! [backend v]     "Persist `v`, replacing whatever was stored. Returns `v`.")
  (clear! [backend]         "Remove the persisted value. Returns nil.")
  (close-backend [backend]  "Release backend-held resources. Returns nil."))

(defprotocol StoredValue
  "Optional. A backend that can persist nil implements this so a stored nil is
  told apart from no value at all; load-state returns nil for both. Without it,
  duratom.core treats a nil from load-state as nothing stored (false is always a
  value)."
  (stored? [backend] "True when the backend holds a persisted value, nil included."))

;; --- file backend -------------------------------------------------------------

(defrecord FileBackend [path]
  StorageBackend
  (load-state [_] (ut/read-edn-file path))
  (persist! [_ v] (ut/write-edn-file path v))
  (clear! [_] (when (fs/exists? path) (fs/delete path)) nil)
  (close-backend [_] nil)
  StoredValue
  (stored? [_] (ut/edn-file-present? path)))

(defn file-backend
  "A backend that persists a single EDN value to `path` on the local filesystem."
  [path]
  (->FileBackend path))

;; --- SQL backend --------------------------------------------------------------

(defrecord SQLBackend [dbspec table row-id conn]
  StorageBackend
  (load-state [_]
    (when-let [row (jdbc/fetch-one conn [(str "select value from " table " where id = ?") row-id])]
      (ut/read-edn-string (:value row))))
  (persist! [_ v]
    (let [s (ut/pr-str-fully v)
          n (jdbc/update! conn (keyword table) {:value s} ["id = ?" row-id])]
      (when (zero? n)
        (jdbc/insert! conn (keyword table) {:id row-id :value s}))
      v))
  (clear! [_]
    (jdbc/delete! conn (keyword table) ["id = ?" row-id])
    nil)
  (close-backend [_]
    (.close conn)
    nil)
  StoredValue
  (stored? [_]
    (some? (jdbc/fetch-one conn [(str "select id from " table " where id = ?") row-id]))))

(defn sql-backend
  "A backend that persists a single EDN value to `table` (columns id, value) over
  a jolt-lang/db connection spec: a \"sqlite:path\" / \"sqlite::memory:\" string,
  a `{:vendor :sqlite :name ...}` map, or a postgres uri. Creates the table if
  missing. The connection is held open for the backend's lifetime, so an
  in-memory sqlite spec keeps its data across mutations."
  ([dbspec table] (sql-backend dbspec table 0))
  ([dbspec table row-id]
   (let [conn (jdbc/connection dbspec)]
     (jdbc/execute! conn (str "create table if not exists " table
                              " (id integer primary key, value text not null)"))
     (->SQLBackend dbspec table row-id conn))))
