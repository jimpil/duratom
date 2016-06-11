(ns duratom.backends
  (:require [duratom.utils :as ut]
            [clojure.java.io :as jio])
  (:import (java.io File IOException)))

(defprotocol IStorageBackend
  (snapshot [this])
  (commit [this])
  (cleanup [this]))

;; ===============================<LOCAL FILE>===========================================
(defn- save-to-file! [path state-atom]
  (let [tmp-file-name (str path ".tmp")]
    (ut/write-edn-to-file! @state-atom tmp-file-name) ;; write data to a temp file
    (ut/move-file! tmp-file-name path)                ;; and rename it atomically
    state-atom))

(defrecord FileBackend [^File file committer]
  IStorageBackend
  (snapshot [_]
    (let [path (.getPath file)]
      (if (.canWrite file)
        (let [file-length (.length file)
              empty-file? (zero? file-length)
              contents (delay (ut/read-edn-from-file! path))]
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

(defn- save-to-db! [db-config table-name state-atom]
  (ut/update-or-insert! db-config table-name {:id 0 :value (pr-str @state-atom)} ["id = ?" 0])
  state-atom)

(defrecord PGSQLBackend [config table-name committer]
  IStorageBackend
  (snapshot [_]
    (ut/get-value config table-name))
  (commit [_]
    (send-off committer (partial save-to-db! config table-name)))
  (cleanup [_]
    (ut/delete-dedicated-table! config table-name)) ;;drop the whole table
  )