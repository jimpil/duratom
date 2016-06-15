(ns duratom.core
  (:require [duratom.backends :as storage]
            [duratom.utils :as ut]
            [clojure.java.io :as jio])
  (:import (clojure.lang IAtom IDeref IRef ARef)
           (java.util.concurrent.locks ReentrantLock)
           (java.io IOException Writer)
           (clojure.lang ARef IFn$D)))

(defmacro ^:private maybe-lock [lock & body]
  (if lock
    `(ut/with-locking ~lock ~@body)
    `(do ~@body)))
;; ================================================================
(defprotocol IDurable
  (write_out [this])
  (read_in [this])
  (destroy [this]))


(defrecord Duratom
  [storage-backend underlying-atom lock release]
  IDurable ;; 2 polymorphic levels here
  (write_out [_]
    (storage/commit storage-backend))
  (read_in [this]
    ;; reset the underlying atom directly to avoid writing exactly what was read in
    (reset! underlying-atom (storage/snapshot storage-backend)))
  (destroy [_]
    (storage/cleanup storage-backend)
    (release true)) ;; the only place where this is called with an argument

  IAtom
  (swap [this f]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (swap! underlying-atom f)]
        (write_out this)
        result)))
  (swap [this f arg]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (swap! underlying-atom f arg)]
        (write_out this)
        result)))
  (swap [this f arg1 arg2]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (swap! underlying-atom f arg1 arg2)]
        (write_out this)
        result)))
  (swap [this f x y args]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (swap! underlying-atom f x y args)]
        (write_out this)
        result)))
  (compareAndSet [this oldv newv]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (compare-and-set! underlying-atom oldv newv)]
        (when result
          (write_out this))
        result)))
  (reset [this newval]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (reset! underlying-atom newval)]
        (write_out this)
        result)))
  IRef
  (setValidator [this validator]
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

;; override default `print-method` for records in order to hide certain fields,
;; but also to provide printing which resembles atoms
(defmethod print-method Duratom [dura ^Writer w]
  (.write w "#")
  (.write w (-> dura class .getName))
  (.write w (format " 0x%x " (System/identityHashCode dura)))
  (.write w " {:status :ready, :val ")
  (.write w (-> dura :underlying-atom deref pr-str))
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
        storage-init (read_in duratom)]
    (if (some? storage-init) ;; found stuff
      duratom
      (cond-> duratom
              (some? init) (doto (reset! init)))))) ;; empty storage means we start off with <initial-value>

(defn- map->Duratom [m]
  (let [[make-backend lock initial-value] ((juxt :make-backend :lock :init) m)]
    (->Duratom make-backend lock initial-value)))


;;==================<PUBLIC API>==========================
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


