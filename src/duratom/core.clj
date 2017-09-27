(ns duratom.core
  (:require [duratom.backends :as storage]
            [duratom.utils :as ut]
            [clojure.java.io :as jio]
            [clojure.edn :as edn]
            [taoensso.nippy :as nippy])
  (:import (clojure.lang IAtom IDeref IRef ARef IMeta IObj Atom)
           (java.util.concurrent.locks ReentrantLock Lock)
           (java.io IOException Writer DataInputStream)))

(defmacro ^:private maybe-lock
  "If your backend is a DB - that has its own lock"
  [lock & body]
  `(if-let [l# ~lock]
     (ut/with-locking l# ~@body)
     (do ~@body)))
;; ================================================================

(deftype Duratom
  [storage-backend ^Atom underlying-atom ^Lock lock release _meta]

  IAtom
  (swap [_ f]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (.swap underlying-atom f)]
        (storage/commit storage-backend)
        result)))
  (swap [_ f arg]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (.swap underlying-atom f arg)]
        (storage/commit storage-backend)
        result)))
  (swap [_ f arg1 arg2]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (.swap underlying-atom f arg1 arg2)]
        (storage/commit storage-backend)
        result)))
  (swap [_ f x y args]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (.swap underlying-atom f x y args)]
        (storage/commit storage-backend)
        result)))
  (compareAndSet [_ oldv newv]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (.compareAndSet underlying-atom oldv newv)]
        (when result
          (storage/commit storage-backend))
        result)))
  (reset [_ newval]
    (ut/assert-not-released! release)
    (maybe-lock lock
      (let [result (.reset underlying-atom newval)]
        (storage/commit storage-backend)
        result)))
  IRef
  (setValidator [_ validator]
    (.setValidator underlying-atom validator))
  (getValidator [_]
    (.getValidator underlying-atom))
  (addWatch [this watch-key watch-fn]
    (.addWatch underlying-atom watch-key watch-fn)
    this)
  (removeWatch [this watch-key]
    (.removeWatch underlying-atom watch-key)
    this)
  (getWatches [_]
    (.getWatches ^ARef underlying-atom))
  IDeref
  (deref [_]
    (.deref underlying-atom))
  IObj
  (withMeta [_ meta-map]
    (Duratom. storage-backend underlying-atom ^Lock lock release meta-map))
  IMeta
  (meta [_]
    _meta)
  )

;; provide a `print-method` that resembles Clojure atoms
(defmethod print-method Duratom [^Duratom dura ^Writer w]
  (.write w "#")
  (.write w (-> dura class .getName))
  (.write w (format " 0x%x " (System/identityHashCode dura)))
  (.write w " {:status :ready, :val ")
  (.write w (-> (.-underlying_atom dura) deref pr-str))
  (.write w "}")
  )


(defn- ->Duratom
  [make-backend lock init]
  (assert (or (ut/lock? lock)
              (nil? lock))
          "The <lock> provided is neither a valid implementation of `java.util.concurrent.locks.Lock`, nor nil!")
  (let [raw-atom (atom nil)
        backend (make-backend (agent raw-atom))
        duratom (Duratom. backend raw-atom lock (ut/releaser) nil)
        storage-init (storage/snapshot backend)]
    (if (some? storage-init) ;; found stuff - sync it
      (do ;; reset the raw atom directly to avoid writing exactly what was read
        (reset! raw-atom storage-init)
        duratom)
      (cond-> duratom
              (some? init) (doto (reset! init)))))) ;; empty storage means we start off with <initial-value>

(defn- map->Duratom [m]
  (let [[make-backend lock initial-value] ((juxt :make-backend :lock :init) m)]
    (->Duratom make-backend lock initial-value)))


;;==================<PUBLIC API>==========================

(defn destroy
  "Convenience fn for cleaning up the persistent storage of a duratom."
  [^Duratom dura]
  (let [storage (.-storage_backend dura)
        release (.-release dura)]
    (storage/cleanup storage)
    (release true)))


(def ^:private default-file-rw
  {:read  ut/read-edn-from-file!
   ;; for nippy use `nippy/thaw-from-file`
   :write ut/write-edn-to-file!
   ;; for nippy use `nippy/freeze-to-file`
   })

(defn file-atom
  "Creates and returns a file-backed atom (on the local filesystem). If the file exists,
   it is read and becomes the initial value. Otherwise, the initial value is <init> and the file <file-path> is updated."
  ([file-path]
   (file-atom file-path (ReentrantLock.) nil))
  ([file-path lock initial-value]
   (file-atom file-path lock initial-value default-file-rw))
  ([file-path lock initial-value rw] ;;read-write details
   (map->Duratom {:lock lock ;; allow for explicit nil
                  :init initial-value
                  :make-backend (partial storage/->FileBackend
                                  (try
                                    (doto (jio/file file-path)
                                          (.createNewFile))
                                    (catch IOException exception
                                      (throw (ex-info "Error creating the required file on the file-system!"
                                                      {:file-path file-path}
                                                      exception))))
                                         (:read rw)
                                         (:write rw))
                  })))


(def ^:private default-postgres-rw
  {:read edn/read-string
   ;; for nippy use `nippy/thaw`
   :write pr-str
   ;; for nippy use `nippy/freeze`
   :column-type :text
   ;; for nippy use :bytea
   })

(defn postgres-atom
  "Creates and returns a PostgreSQL-backed atom. If the location denoted by the combination of <db-config> and <table-name> exists,
   it is read and becomes the initial value. Otherwise, the initial value is <init> and the table <table-name> is updated."
  ([db-config table-name]
   (postgres-atom db-config table-name 0 (ReentrantLock.) nil))
  ([db-config table-name row-id]
   (postgres-atom db-config table-name row-id (ReentrantLock.) nil))
  ([db-config table-name row-id lock initial-value]
   (postgres-atom db-config table-name row-id lock initial-value default-postgres-rw))
  ([db-config table-name row-id lock initial-value rw]
   (map->Duratom {:lock lock
                  :init initial-value
                  :make-backend (partial storage/->PGSQLBackend
                                         db-config
                                         (if (ut/table-exists? db-config table-name)
                                           table-name
                                           (do (ut/create-dedicated-table! db-config table-name (:column-type rw))
                                               table-name))
                                         row-id
                                         (:read rw)
                                         (:write rw))
                  })))

(def ^:private default-s3-rw
  {:read  ut/read-edn-from-file!
   ;; for nippy use `#(with-open [di (DataInputStream. %)] (nippy/thaw-from-in! di))`
   :write pr-str
   ;; for nippy use `nippy/freeze
   })

(defn s3-atom
  "Creates and returns an S3-backed atom. If the location denoted by the combination of <bucket> and <k> exists,
   it is read and becomes the initial value. Otherwise, the initial value is <init> and the bucket key <k> is updated."
  ([creds bucket k]
   (s3-atom creds bucket k (ReentrantLock.) nil))
  ([creds bucket k lock initial-value]
   (s3-atom creds bucket k lock initial-value default-s3-rw))
  ([creds bucket k lock initial-value rw]
   (map->Duratom {:lock         lock
                  :init         initial-value
                  :make-backend (partial storage/->S3Backend
                                         creds
                                         (if (ut/bucket-exists? creds bucket)
                                           bucket
                                           (do (ut/create-s3-bucket creds bucket)
                                               bucket))
                                         k
                                         (:read rw)
                                         (:write rw))
                  })))


(defmulti duratom
  "Top level constructor function for the <Duratom> class.
   Built-in <backed-by> types are `:local-file`, `:postgres-db` & `:aws-s3`."
  (fn [backed-by & _args]
    backed-by))

(defmethod duratom :local-file
  [_ & {:keys [file-path init lock rw]
        :or {lock (ReentrantLock.)
             rw default-file-rw}}]
  (file-atom file-path lock init rw))

(defmethod duratom :postgres-db
  [_ & {:keys [db-config table-name row-id init lock rw]
        :or {lock (ReentrantLock.)
             rw default-postgres-rw}}]
  (postgres-atom db-config table-name row-id lock init rw))

(defmethod duratom :aws-s3
  [_ & {:keys [credentials bucket key init lock rw]
        :or {lock (ReentrantLock.)
             rw default-s3-rw}}]
  (s3-atom credentials bucket key lock init rw))


