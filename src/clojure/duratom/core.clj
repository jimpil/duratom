(ns duratom.core
  (:require [duratom.backends :as storage]
            [duratom.utils :as ut]
            [clojure.java.io :as jio]
            [duratom.readers :as readers]
            [duratom.utils :as ut])
  (:import (clojure.lang IAtom IDeref IRef ARef IMeta IObj Atom IAtom2 Agent IReference)
           (java.util.concurrent.locks ReentrantLock Lock)
           (java.io Writer Closeable)))
;; ================================================================

(deftype Duratom
  [storage-backend ^Atom underlying-atom ^Lock lock release]

  IAtom2 ;; the new interface introduced in 1.9
  (swapVals [_ f]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [[_ n :as result] (.swapVals underlying-atom f)]
        (storage/commit storage-backend n)
        result)))
  (swapVals [_ f arg1]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [[_ n :as result] (.swapVals underlying-atom f arg1)]
        (storage/commit storage-backend n)
        result)))
  (swapVals [_ f arg1 arg2]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [[_ n :as result] (.swapVals underlying-atom f arg1 arg2)]
        (storage/commit storage-backend n)
        result)))
  (swapVals [_ f arg1 arg2 more]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [[_ n :as result] (.swapVals underlying-atom f arg1 arg2 more)]
        (storage/commit storage-backend n)
        result)))
  (resetVals [_ newvals]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [[_ n :as result] (.resetVals underlying-atom newvals)]
        (storage/commit storage-backend n)
        result)))

  IAtom
  (swap [_ f]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (.swap underlying-atom f)]
        (storage/commit storage-backend result)
        result)))
  (swap [_ f arg]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (.swap underlying-atom f arg)]
        (storage/commit storage-backend result)
        result)))
  (swap [_ f arg1 arg2]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (.swap underlying-atom f arg1 arg2)]
        (storage/commit storage-backend result)
        result)))
  (swap [_ f arg1 arg2 more]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (.swap underlying-atom f arg1 arg2 more)]
        (storage/commit storage-backend result)
        result)))
  (compareAndSet [_ oldv newv]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (.compareAndSet underlying-atom oldv newv)]
        (when result
          (storage/commit storage-backend result))
        result)))
  (reset [_ newval]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (.reset underlying-atom newval)]
        (storage/commit storage-backend result)
        result)))
  IRef ;; watches/validators/meta/deref works against the underlying atom
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
    (Duratom. storage-backend (with-meta underlying-atom meta-map) ^Lock lock release))
  IMeta
  (meta [_]
    (meta underlying-atom))
  IReference
  (resetMeta [_ meta-map]
    (.resetMeta underlying-atom meta-map))
  (alterMeta [_ f args]
    (.alterMeta underlying-atom f args))
  Closeable
  (close [_]
    (storage/safe-cleanup! storage-backend release lock))
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

(defonce DEFAULT_COMMIT_MODE ::async)

(defn- async?
  "Returns true if the commit-mode <cmode>
   provided is either nil or ::async."
  [cmode]
  (or (= cmode DEFAULT_COMMIT_MODE)
      (nil? cmode)))

(defn- recommit-fn
  [backend]
  (fn recommit* []
    ;; recommitting has to access (deref) the state
    ;; but that's ok because we're either within a lock,
    ;; or
    (storage/commit backend)))

(defn- add-atom-error-handler
  [backend handle-error]
  (let [p (promise)
        rcmt (delay (recommit-fn @p))]
    @(deliver p
             (with-meta backend
                        {:error-handler
                         (if (nil? handle-error)
                           ut/noop
                           (fn [e]
                             (try (handle-error e @rcmt)
                                  ;; swallow error-handler exceptions
                                  ;; much like an agent would
                                  (catch Exception _
                                    ;(println "swallowed" _)
                                    nil))))}))))

(defn- add-agent-error-handler
  [backend handle-error]
  (set-error-handler!
    (:committer backend) ;; the agent
    (if (nil? handle-error)
      ut/noop
      (let [recommit* (recommit-fn
                        ;; force synchronous recommit (so it's as safe as possible)
                        (with-meta backend
                                   {:error-handler
                                    ;; let this bubble up so that the
                                    ;; user error-handler deals with it
                                    ;; or the agent eventually swallows it
                                    (fn [e] (throw e))}))]
        (fn [_ e]
          ;; recommitting happens on the
          ;; agent's dispatch thread *synchronously* (wrt the original commit)
          (handle-error e recommit*)))))
  backend)

(defn- ->Duratom
  ([make-backend lock init commits]
   (->Duratom make-backend lock init commits nil))
  ([make-backend lock init commits handle-error]
   (assert (ut/lock? lock)
           "The <lock> provided is NOT a valid implementation of `java.util.concurrent.locks.Lock`!")
   (let [raw-atom (atom nil)
         async-commits? (async? commits)
         backend* (cond-> raw-atom
                          async-commits? (agent :error-mode :continue)
                          true           make-backend)
         backend (if async-commits?
                   ;; need to do this on a separate step
                   (add-agent-error-handler backend* handle-error)
                   (add-atom-error-handler backend* handle-error))
         duratom (Duratom. backend raw-atom lock (ut/releaser))
         storage-init (storage/snapshot backend)]
     (if (some? storage-init) ;; found stuff - sync it
       (do ;; reset the raw atom directly to avoid writing exactly what was read
         (reset! raw-atom storage-init)
         duratom)
       (cond-> duratom
               (some? init)
               (doto (reset! (ut/->init init)))))))) ;; empty storage means we start off with <initial-value>

(defn- map->Duratom [m]
  (let [{:keys [make-backend lock init commit-mode error-handler]} m]
    (->Duratom make-backend lock init commit-mode error-handler)))


;;==================<PUBLIC API>==========================

(defn destroy
  "Convenience fn for cleaning up the persistent storage
  of a duratom/duragent manually."
  [dura]
  (condp instance? dura
    Duratom (.close ^Closeable dura) ;; duratom implements Closeable
    Agent   (when-let [cleanup! (some-> (meta dura) ::storage/destroy)]
              (cleanup!))))          ;; duragent doesn't

(defn backend-snapshot
  "Convenience fn for acquiring a snapshot of
   the persistent storage of a duratom/duragent manually."
  [dura]
  (condp instance? dura
    Duratom (storage/snapshot (.-storage_backend dura))
    Agent (when-let [snap (some-> (meta dura) ::storage/snapshot)]
            (snap))))


(def default-file-rw
  {:read  ut/read-edn-object  ;; for nippy use `nippy/thaw-from-file`
   :write ut/write-edn-object ;; for nippy use `nippy/freeze-to-file`
   :commit-mode DEFAULT_COMMIT_MODE} ;; technically not needed but leaving it for transparency
  )

(defn file-atom
  "Creates and returns a file-backed atom (on the local filesystem). If the file exists,
   it is read and becomes the initial value. Otherwise, the initial value is <init> and the file <file-path> is updated."
  ([file-path]
   (file-atom file-path (ReentrantLock.) nil))
  ([file-path lock initial-value]
   (file-atom file-path lock initial-value default-file-rw))
  ([file-path lock initial-value rw] ;;read-write details
   (map->Duratom (merge
                   {:lock lock ;; allow for explicit nil
                    :init initial-value
                    :make-backend (partial storage/->FileBackend
                                           (doto (jio/file file-path)
                                             (.createNewFile))
                                           (:read rw)
                                           (:write rw))}
                   (select-keys rw [:commit-mode :error-handler])))))


(def default-postgres-rw
  {:read  ut/read-edn-string             ;; for nippy use `nippy/thaw`
   :write (partial ut/pr-str-fully true) ;; for nippy use `nippy/freeze`
   :column-type :text                    ;; for nippy use :bytea
   :commit-mode DEFAULT_COMMIT_MODE} ;; technically not needed but leaving it for transparency
  )

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
   (map->Duratom (merge
                   {:lock lock
                    :init initial-value
                    :make-backend (partial storage/->PGSQLBackend
                                           db-config
                                           (if (ut/table-exists? db-config table-name)
                                             table-name
                                             (do (ut/create-dedicated-table! db-config table-name (:column-type rw))
                                                 table-name))
                                           row-id
                                           (:read rw)
                                           (:write rw))}
                   (select-keys rw [:commit-mode :error-handler])))))

(def default-s3-rw
  ;; `edn/read` doesn't make use of the object size, so no reason to fetch it from S3 (we communicate that via metadata).
  ;; Contrast that with `ut/s3-bucket-bytes` which needs to copy the bytes from the S3 input-stream to some output-stream
  ;; (using an internal buffer). In such cases (e.g. nippy encoded bytes), knowing the object size means we can avoid copying entirely.
  {:read (with-meta
           (partial ut/read-edn-object readers/default) ;; this will be called with two args
           {:ignore-size? true}) ;; for nippy use `(comp nippy/thaw ut/s3-bucket-bytes)`
   :write      (partial ut/pr-str-fully true)  ;; for nippy use `nippy/freeze`
   :commit-mode DEFAULT_COMMIT_MODE ;; technically not needed but leaving it for transparency
   ;:metadata {:server-side-encryption "AES256"}
   })

(defn s3-atom
  "Creates and returns an S3-backed atom. If the location denoted by the combination of <bucket> and <k> exists,
   it is read and becomes the initial value. Otherwise, the initial value is <init> and the bucket key <k> is updated."
  ([creds bucket k]
   (s3-atom creds bucket k (ReentrantLock.) nil))
  ([creds bucket k lock initial-value]
   (s3-atom creds bucket k lock initial-value default-s3-rw))
  ([creds bucket k lock initial-value rw]
   (map->Duratom (merge
                   {:lock lock
                    :init initial-value
                    :make-backend (partial storage/->S3Backend
                                           creds
                                           (if (ut/bucket-exists? creds bucket)
                                             bucket
                                             (do (ut/create-s3-bucket creds bucket)
                                                 bucket))
                                           k
                                           (:metadata rw)
                                           (:read rw)
                                           (:write rw))}
                   (select-keys rw [:commit-mode :error-handler])))))

(def default-redis-rw
  {;; Redis library Carmine automatically uses Nippy for serialization/deserialization Clojure types
   ;; So by just replacing these functions with `identity` they will be serialized with Nippy
   :read  ut/read-edn-string
   :write (partial ut/pr-str-fully true)
   :commit-mode DEFAULT_COMMIT_MODE}) ;; technically not needed but leaving it for transparency)

(defn redis-atom
  "Creates and returns a Redis-backed atom. If the location denoted by the combination of <db-config> and <key-name> exists,
  it is read and becomes the initial value. Otherwise, the initial value is <init> and the key <key-name> is updated."
  ([db-config key-name]
   (redis-atom db-config key-name (ReentrantLock.) nil))
  ([db-config key-name lock initial-value]
   (redis-atom db-config key-name lock initial-value default-redis-rw))
  ([db-config key-name lock initial-value rw]
   (map->Duratom (merge
                   {:lock lock
                    :init initial-value
                    :make-backend (partial storage/->RedisBackend
                                           db-config
                                           key-name
                                           (:read rw)
                                           (:write rw))}
                   (select-keys rw [:commit-mode :error-handler])))))

(defmulti duratom
          "Top level constructor function for the <Duratom> class.
   Built-in <backed-by> types are `:local-file`, `:postgres-db`, `:redis-db` & `:aws-s3`."
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

(defmethod duratom :redis-db
  [_ & {:keys [db-config key-name init lock rw]
        :or {lock (ReentrantLock.)
             rw default-redis-rw}}]
  (redis-atom db-config key-name lock init rw))
;;=========================================================
;;=======================DURAGENT==========================

(defn- duragent*
  "Common constructor for duragents"
  [init ehandler make-backend]
  (let [ag (agent nil :error-mode :continue)
        backend (with-meta (make-backend ag)
                           ;; force synchronous recommits
                           {:error-handler (fn [e] (throw e))})
        storage-init (storage/snapshot backend)
        cleanup-lock (ReentrantLock.)
        release (ut/releaser)
        final-agent (delay
                      (-> ag
                          (add-watch ::storage/commit
                             (fn [_ _ o n]
                               (when (not= o n)
                                 (ut/assert-not-released! release)
                                 (try
                                   (storage/commit backend n)
                                   (catch Exception e
                                     (throw
                                       (ex-info (str "Commit error: " e)
                                                {:type ::storage/commit-error}
                                                e)))))))
                          (doto
                            (reset-meta! {::storage/destroy  (partial storage/safe-cleanup! backend release cleanup-lock)
                                          ::storage/snapshot #(storage/snapshot backend)})
                            (set-error-handler!
                              (if (nil? ehandler)
                                ut/noop
                                (fn [a e]
                                  (if (= ::storage/commit-error (some-> (ex-data e) :type))
                                    (ehandler a (ex-cause e) #(storage/commit backend @a))
                                    (ehandler a e))))))))]
    (if (some? storage-init)
      ;; found stuff - sync it before adding the commit watch
      (send-off ag (constantly storage-init))
      ;; empty storage - trigger first commit
      (send-off @final-agent (constantly (ut/->init init))))
    @final-agent))

(defmulti duragent
          "duragent is to agent, what duratom is to atom"
          (fn [backed-by & _args]
            backed-by))

(defmethod duragent :local-file
  [_ & {:keys [file-path init rw]
        :or {rw default-file-rw}}]
  (let [make-backend (partial storage/->FileBackend
                              (doto (jio/file file-path)
                                (.createNewFile))
                              (:read rw)
                              (:write rw))]
    (duragent* init (:error-handler rw) make-backend)))

(defmethod duragent :postgres-db
  [_ & {:keys [db-config table-name row-id init lock rw]
        :or {lock (ReentrantLock.)
             rw default-postgres-rw}}]
  (let [make-backend (partial storage/->PGSQLBackend
                              db-config
                              (if (ut/table-exists? db-config table-name)
                                table-name
                                (do (ut/create-dedicated-table! db-config table-name (:column-type rw))
                                    table-name))
                              row-id
                              (:read rw)
                              (:write rw))]
    (duragent* init (:error-handler rw) make-backend)))

(defmethod duragent :aws-s3
  [_ & {:keys [credentials bucket key init lock rw]
        :or {lock (ReentrantLock.)
             rw default-s3-rw}}]
  (let [make-backend (partial storage/->S3Backend
                              credentials
                              (if (ut/bucket-exists? credentials bucket)
                                bucket
                                (do (ut/create-s3-bucket credentials bucket)
                                    bucket))
                              key
                              (:metadata rw)
                              (:read rw)
                              (:write rw))]
    (duragent* init (:error-handler rw) make-backend)))

(defmethod duragent :redis-db
  [_ & {:keys [db-config key-name init lock rw]
        :or {lock (ReentrantLock.)
             rw default-redis-rw}}]
  (let [make-backend (partial storage/->RedisBackend
                              db-config
                              key-name
                              (:read rw)
                              (:write rw))]
    (duragent* init (:error-handler rw) make-backend)))
