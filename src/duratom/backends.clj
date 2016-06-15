(ns duratom.backends
  (:require [duratom.utils :as ut])
  (:import (java.io File IOException)))

(defprotocol IStorageBackend
  (snapshot [this])
  (commit [this])
  (cleanup [this]))

;; ===============================<LOCAL FILE>===========================================
(defn- save-to-file! [path state-atom]
  (let [tmp-file-name (str path ".tmp")]
    (ut/write-edn! @state-atom tmp-file-name) ;; write data to a temp file
    (ut/move-file! tmp-file-name path)        ;; and move it atomically
    state-atom))

(defrecord FileBackend [^File file committer]
  IStorageBackend
  (snapshot [_]
    (let [path (.getPath file)]
      (if (.canWrite file)
        (let [file-length (.length file)
              empty-file? (zero? file-length)
              contents (delay (ut/read-edn! path))]
          (when-not empty-file?
            (try (force contents)
                 (catch Exception e
                   (throw (ex-info (str "Unable to read data from file " path "!")
                                   {:file-path path}
                                   e))))))
        (throw (ex-info "The file backing the duratom must be writeable!" {:file-path path})))))
  (commit [_]
    (send-off committer (partial save-to-file! (.getPath file))))
  (cleanup [_]
    (or (.delete file)
        (throw (IOException. (str "Could not delete " (.getPath file)))))) ;; simply delete the file
  )

;;===================================<PGSQL>=====================================

(defn- save-to-db! [db-config table-name row-id state-atom]
  (ut/update-or-insert! db-config table-name {:id row-id :value (pr-str @state-atom)} ["id = ?" row-id])
  state-atom)

(defrecord PGSQLBackend [config table-name row-id committer]
  IStorageBackend
  (snapshot [_]
    (ut/get-pgsql-value config table-name row-id))
  (commit [_]
    (send-off committer (partial save-to-db! config table-name row-id)))
  (cleanup [_]
    (ut/delete-relevant-row! config table-name row-id)) ;;drop the relevant row
  )

;;==========================<AMAZON-S3>=============================================

(defn- save-to-s3! [credentials bucket k state-atom]
  (ut/store-value-to-s3 credentials bucket k (pr-str @state-atom))
  state-atom)

(defrecord S3Backend [credentials bucket k committer]
  IStorageBackend
  (snapshot [_]
    (ut/get-value-from-s3 credentials bucket k))
  (commit [_]
    (send-off committer (partial save-to-s3! credentials bucket k)))
  (cleanup [_]
    (ut/delete-object-from-s3 credentials bucket k)) ;;drop the whole object
  )