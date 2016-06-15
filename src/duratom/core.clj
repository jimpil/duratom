(ns duratom.core
  (:require [duratom.backends :as storage]
            [duratom.utils :as ut]
            [clojure.java.io :as jio])
  (:import (clojure.lang IAtom IDeref IRef ARef)
           (java.util.concurrent.locks ReentrantLock)
           (java.io IOException Writer)))

(defmacro ^:private maybe-lock [lock & body]
  (if lock
    `(ut/with-locking ~lock ~@body)
    `(do ~@body)))
;; ================================================================

(deftype Duratom
  [storage-backend underlying-atom lock release]

  IAtom
  (swap [_ f]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (swap! underlying-atom f)]
        (storage/commit storage-backend)
        result)))
  (swap [_ f arg]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (swap! underlying-atom f arg)]
        (storage/commit storage-backend)
        result)))
  (swap [_ f arg1 arg2]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (swap! underlying-atom f arg1 arg2)]
        (storage/commit storage-backend)
        result)))
  (swap [_ f x y args]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (swap! underlying-atom f x y args)]
        (storage/commit storage-backend)
        result)))
  (compareAndSet [_ oldv newv]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (compare-and-set! underlying-atom oldv newv)]
        (when result
          (storage/commit storage-backend))
        result)))
  (reset [_ newval]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (reset! underlying-atom newval)]
        (storage/commit storage-backend)
        result)))
  IRef
  (setValidator [_ validator]
    (set-validator! underlying-atom validator))
  (getValidator [_]
    (get-validator underlying-atom))
  (addWatch [this watch-key watch-fn]
    (add-watch underlying-atom watch-key watch-fn)
    this)
  (removeWatch [this watch-key]
    (remove-watch underlying-atom watch-key)
    this)
  (getWatches [_]
    (.getWatches ^ARef underlying-atom))
  IDeref
  (deref [_]
    @underlying-atom)
  )

;; provide a `print-method` that resembles Clojure atoms
(defmethod print-method Duratom [dura ^Writer w]
  (.write w "#")
  (.write w (-> dura class .getName))
  (.write w (format " 0x%x " (System/identityHashCode dura)))
  (.write w " {:status :ready, :val ")
  (.write w (-> (.-underlying_atom dura) deref pr-str))
  (.write w "}")
  )


(defn- ->Duratom ;; shadow the generated constructor-fns
  [make-backend lock init]
  (assert (or (ut/lock? lock)
              (nil? lock))
          "The <lock> provided is neither a valid implementation of `java.util.concurrent.locks.Lock`, nor nil!")
  (let [raw-atom (atom nil)
        backend (make-backend (agent raw-atom))
        duratom (Duratom. backend raw-atom lock (ut/releaser))
        storage-init (storage/snapshot backend)]
    (if (some? storage-init) ;; found stuff - sync it
      (do ;; reset the raw atom directly to avoid writing exactly what was read in
        (reset! raw-atom storage-init)
        duratom)
      (cond-> duratom
              (some? init) (doto (reset! init)))))) ;; empty storage means we start off with <initial-value>

(defn- map->Duratom [m]
  (let [[make-backend lock initial-value] ((juxt :make-backend :lock :init) m)]
    (->Duratom make-backend lock initial-value)))


;;==================<PUBLIC API>==========================

(defn destroy
  "Convenience fn for cleaning up the persistent storage of a duratom.
  In absence of this fn, one would have to go via `duratom.backends`."
  [^Duratom dura]
  (let [storage (.-storage_backend dura)
        release (.-release dura)]
    (storage/cleanup storage)
    (release true)))


(defn file-atom
  "Creates and returns a file-backed atom (on the local filesystem). If the file exists,
   it is read and becomes the initial value. Otherwise, the initial value is <init> and the file <file-path> is updated."
  ([file-path]
   (file-atom file-path (ReentrantLock.) nil))
  ([file-path lock initial-value]
   (map->Duratom {:lock lock ;; allow for explicit nil
                  :init initial-value
                  :make-backend (partial storage/->FileBackend
                                         (try
                                           (doto (jio/file file-path)
                                             (.createNewFile))
                                           (catch IOException exception
                                             (throw (ex-info "Failed creating the file needed!"
                                                             {:file-path file-path}
                                                             exception)))))
                  })))



(defn postgres-atom
  "Creates and returns a PostgreSQL-backed atom. If the location denoted by the combination of <db-config> and <table-name> exists,
   it is read and becomes the initial value. Otherwise, the initial value is <init> and the table <table-name> is updated."
  ([db-config table-name]
   (postgres-atom db-config table-name 0 (ReentrantLock.) nil))
  ([db-config table-name row-id]
   (postgres-atom db-config table-name row-id (ReentrantLock.) nil))
  ([db-config table-name row-id lock initial-value]
   (map->Duratom {:lock lock
                  :init initial-value
                  :make-backend (partial storage/->PGSQLBackend
                                         db-config
                                         (if (ut/table-exists? db-config table-name)
                                           table-name
                                           (do (ut/create-dedicated-table! db-config table-name)
                                               table-name))
                                         row-id)
                  })))

(defn s3-atom
  "Creates and returns an S3-backed atom. If the location denoted by the combination of <bucket> and <k> exists,
   it is read and becomes the initial value. Otherwise, the initial value is <init> and the bucket key <k> is updated."
  ([creds bucket k]
   (s3-atom creds bucket k (ReentrantLock.) nil))
  ([creds bucket k lock initial-value]
   (map->Duratom {:lock lock
                  :init initial-value
                  :make-backend (partial storage/->S3Backend
                                         creds
                                         (if (ut/does-bucket-exist creds bucket)
                                           bucket
                                           (do (ut/create-s3-bucket creds bucket)
                                               bucket))
                                         k)
                  })))


(defmulti duratom
  "Top level constructor function for the <Duratom> class.
   Built-in <backed-by> types are `:local-file`, `:postgres-db` & `:aws-s3`."
  (fn [backed-by & _args]
    backed-by))

(defmethod duratom :local-file
  [_ & {:keys [file-path init lock]
        :or {lock (ReentrantLock.)}}]
  (file-atom file-path lock init))

(defmethod duratom :postgres-db
  [_ & {:keys [db-config table-name row-id init lock]
        :or {lock (ReentrantLock.)}}]
  (postgres-atom db-config table-name row-id lock init))

(defmethod duratom :aws-s3
  [_ & {:keys [credentials bucket key init lock]
        :or {lock (ReentrantLock.)}}]
  (s3-atom credentials bucket key lock init))


