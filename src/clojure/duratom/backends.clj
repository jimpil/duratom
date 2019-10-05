(ns duratom.backends
  (:require [duratom.utils :as ut])
  (:import (java.io File IOException)
           (clojure.lang Agent Atom)))

(defprotocol ICommiter
  (commit! [this f ef]
           [this f]))

(extend-protocol ICommiter
  Agent ;; asynchronous
  (commit!
    ([this f]
     (send-off this f))
    ([this f handle-error]
     ;; do this synchronously!
     (commit! (deref this) f handle-error)))
  Atom  ;; synchronous
  (commit! [this f handle-error]
    (try
      (f this)
      (catch Exception e
        (handle-error e))))
  )

(defprotocol IStorageBackend
  (snapshot [this])
  (commit [this])
  (cleanup [this]))

(defn- get-error-handler
  [backend]
  (some-> (meta backend) :error-handler))

;; ===============================<LOCAL FILE>===========================================
(defn- save-to-file!
  [write-it! path state-atom]
  (let [tmp-file-name (str path ".tmp")]
    (write-it! tmp-file-name @state-atom)     ;; write data to a temp file
    (ut/move-file! tmp-file-name path)        ;; and move it atomically
    state-atom))

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
    ;; synchronous backends always have an error-handler in their meta
    ;; asynchronous ones MAY have one (for synchronous re-committing)
    (let [f (partial save-to-file! write-it! (.getPath file))]
      (if-let [ehandler (get-error-handler this)]
        (commit! committer f ehandler)
        (commit! committer f))))
  (cleanup [_]
    (or (.delete file) ;; simply delete the file
        (throw (IOException. (str "Could not delete " (.getPath file))))))
  )

;;===================================<PGSQL>=====================================

(defn- save-to-db! [db-config table-name row-id write-it! state-atom]
  (ut/update-or-insert! db-config table-name {:id row-id :value (write-it! @state-atom)} ["id = ?" row-id])
  state-atom)

(defrecord PGSQLBackend [config table-name row-id read-it! write-it! committer]
  IStorageBackend
  (snapshot [_]
    (ut/get-pgsql-value config table-name row-id read-it!))
  (commit [this]
    (let [f (partial save-to-db! config table-name row-id write-it!)]
      (if-let [ehandler (get-error-handler this)]
        (commit! committer f ehandler)
        (commit! committer f))))
  (cleanup [_]
    (ut/delete-relevant-row! config table-name row-id)) ;;drop the relevant row
  )

;;==========================<AMAZON-S3>=============================================

(defn- save-to-s3!
  [credentials bucket k write-it! state-atom]
  (ut/store-value-to-s3 credentials bucket k (write-it! @state-atom))
  state-atom)

(defrecord S3Backend [credentials bucket k metadata read-it! write-it! committer]
  IStorageBackend
  (snapshot [_]
    (ut/get-value-from-s3 credentials bucket k metadata read-it!))
  (commit [this]
    (let [f (partial save-to-s3! credentials bucket k write-it!)]
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
    (let [f (fn [state-atom]
              (ut/redis-set conn key-name (write-it! @state-atom))
              state-atom)]
      (if-let [ehandler (get-error-handler this)]
        (commit! committer f ehandler)
        (commit! committer f))))
  (cleanup [_]
    (ut/redis-del conn key-name)))
