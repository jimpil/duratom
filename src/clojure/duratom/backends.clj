(ns duratom.backends
  (:require [duratom.utils :as ut])
  (:import (java.io File IOException)
           (clojure.lang Agent Atom)))

(defprotocol ICommiter
  (commit! [this f ef]
           [this f]))

(defn- sync-commit*
  [c f handle-error]
  (try
    (f c)
    (catch Exception e
      (handle-error e))))

(extend-protocol ICommiter
  Agent  ;; asynchronous
  (commit!
    ([this f]
     (send-off this f))
    ([this f handle-error]
     ;; delegate to the synchronous Atom(applicable to duratom recommits),
     ;; or Object(applicable to all duragent commits) implementation
     (commit! (deref this) f handle-error)))
  Atom   ;; synchronous
  (commit! [this f handle-error]
    (sync-commit* this f handle-error))
  Object ;; synchronous
  (commit! [this f handle-error]
    (sync-commit* this f handle-error))
  )

(defprotocol IStorageBackend
  (snapshot [this])
  (commit   [this] ;; applicable only to duratom re-commits via the error-handler
            [this x])
  (cleanup  [this]))

(defn- get-error-handler
  [backend]
  (some-> (meta backend) :error-handler))

(defn- ?deref
  [state x]
  (if (= ::deref x)
    @state
    x))

;; ===============================<LOCAL FILE>===========================================
(defn- save-to-file!
  [write-it! path v]
  (let [tmp-file-name (str path ".tmp")]
    (write-it! tmp-file-name v)           ;; write data to a temp file
    (ut/move-file! tmp-file-name path)))  ;; and move it atomically

(defrecord FileBackend [^File file read-it! write-it! committer]
  IStorageBackend
  (snapshot [_]
    (let [path (.getPath file)]
      (when-not (zero? (.length file))
        (try (read-it! path)
             (catch Exception e
               (throw (ex-info (str "Unable to read data from file " path "!")
                               {:file-path path}
                               e)))))))
  (commit [this]
    (commit this ::deref)) ;; duratom recommits only down this path
  (commit [this x]
    ;; synchronous backends always have an error-handler in their meta
    ;; asynchronous ones MAY have one (for synchronous re-committing)
    (let [f (fn [state]
              (save-to-file! write-it! (.getPath file) (?deref state x))
              state)]
      (if-let [ehandler (get-error-handler this)]
        (commit! committer f ehandler)
        (commit! committer f))))
  (cleanup [_]
    (or (.delete file) ;; simply delete the file
        (throw (IOException. (str "Could not delete " (.getPath file))))))
  )

;;===================================<PGSQL>=====================================

(defn- save-to-db! [db-config table-name row-id write-it! x]
  (ut/update-or-insert! db-config table-name {:id row-id :value (write-it! x)} ["id = ?" row-id]))

(defrecord PGSQLBackend [config table-name row-id read-it! write-it! committer]
  IStorageBackend
  (snapshot [_]
    (ut/get-pgsql-value config table-name row-id read-it!))
  (commit [this]
    (commit this ::deref))
  (commit [this x]
    (let [f (fn [state]
              (save-to-db! config table-name row-id write-it! (?deref state x))
              state)]
      (if-let [ehandler (get-error-handler this)]
        (commit! committer f ehandler)
        (commit! committer f))))
  (cleanup [_]
    (ut/delete-relevant-row! config table-name row-id)) ;;drop the relevant row
  )

;;==========================<AMAZON-S3>=============================================

(defn- save-to-s3!
  [credentials bucket k write-it! x]
  (ut/store-value-to-s3 credentials bucket k (write-it! x)))

(defrecord S3Backend [credentials bucket k metadata read-it! write-it! committer]
  IStorageBackend
  (snapshot [_]
    (ut/get-value-from-s3 credentials bucket k metadata read-it!))
  (commit [this]
    (commit this ::deref))
  (commit [this x]
    (let [f (fn [state]
              (save-to-s3! credentials bucket k write-it! (?deref state x))
              state)]
      (if-let [ehandler (get-error-handler this)]
        (commit! committer f ehandler)
        (commit! committer f))))
  (cleanup [_]
    (ut/delete-object-from-s3 credentials bucket k)) ;;drop the whole object
  )

;;==========================<REDIS>=============================================

(defrecord RedisBackend [conn key-name read-it! write-it! committer]
  IStorageBackend
  (snapshot [_]
    (read-it! (ut/redis-get conn key-name)))
  (commit [this]
    (commit this ::deref))
  (commit [this x]
    (let [f (fn [state]
              (ut/redis-set conn key-name (write-it! (?deref state x)))
              state)]
      (if-let [ehandler (get-error-handler this)]
        (commit! committer f ehandler)
        (commit! committer f))))
  (cleanup [_]
    (ut/redis-del conn key-name)))
