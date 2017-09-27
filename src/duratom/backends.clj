(ns duratom.backends
  (:require [duratom.utils :as ut])
  (:import (java.io File IOException)))

(defprotocol IStorageBackend
  (snapshot [this])
  (commit [this])
  (cleanup [this]))

;; ===============================<LOCAL FILE>===========================================
(defn- save-to-file!
  [write-it! path state-atom]
  (let [tmp-file-name (str path ".tmp")]
    (write-it! @state-atom tmp-file-name)     ;; write data to a temp file
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
  (commit [_]
    (send-off committer (partial save-to-file! write-it! (.getPath file))))
  (cleanup [_]
    (or (.delete file)
        (throw (IOException. (str "Could not delete " (.getPath file)))))) ;; simply delete the file
  )

;;===================================<PGSQL>=====================================

(defn- save-to-db! [db-config table-name row-id write-it! state-atom]
  (ut/update-or-insert! db-config table-name {:id row-id :value (write-it! @state-atom)} ["id = ?" row-id])
  state-atom)

(defrecord PGSQLBackend [config table-name row-id read-it! write-it! committer]
  IStorageBackend
  (snapshot [_]
    (ut/get-pgsql-value config table-name row-id read-it!))
  (commit [_]
    (send-off committer (partial save-to-db! config table-name row-id write-it!)))
  (cleanup [_]
    (ut/delete-relevant-row! config table-name row-id)) ;;drop the relevant row
  )

;;==========================<AMAZON-S3>=============================================

(defn- save-to-s3!
  [credentials bucket k write-it! state-atom]
  (ut/store-value-to-s3 credentials bucket k (write-it! @state-atom))
  state-atom)

(defrecord S3Backend [credentials bucket k read-it! write-it! committer]
  IStorageBackend
  (snapshot [_]
    (ut/get-value-from-s3 credentials bucket k read-it!))
  (commit [_]
    (send-off committer (partial save-to-s3! credentials bucket k write-it!)))
  (cleanup [_]
    (ut/delete-object-from-s3 credentials bucket k)) ;;drop the whole object
  )