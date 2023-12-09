(ns duratom.core
  (:require [duratom.backends :as storage]
            [duratom.utils :as ut]
            [clojure.java.io :as jio]
            [duratom.readers :as readers]
            [duratom.utils :as ut])
  (:import (clojure.lang IAtom IDeref IRef ARef IMeta IObj IAtom2 Agent IReference)
           (java.util.concurrent.locks ReentrantLock Lock)
           (java.io Writer Closeable)))
;; ================================================================

(defonce DEFAULT_COMMIT_MODE   ::async)
(defonce DEFAULT_READ_LOCATION ::memory)
(def ^:dynamic *atom-ctor*     atom)

(defmacro with-atom-ctor
  "Binds *atom-ctor* to the provided <ctor> (1-arg) fn,
   and executes body. Helpful for creating duratom(s) that
   wrap IAtom(s) other than clojure.lang.Atom - what the
   default one (`clojure.core/atom`) returns."
  [ctor & body]
  `(binding [*atom-ctor* ~ctor] ~@body))

(defn- async?
  "Returns true if the commit-mode <cmode>
   provided is either nil or ::async."
  [cmode]
  (or (= cmode DEFAULT_COMMIT_MODE)
      (nil? cmode)))

(defn- memory-reads?
  "Returns true if the read-from <loc>
   provided is either nil or ::memory."
  [loc]
  (or (= loc DEFAULT_READ_LOCATION)
      (nil? loc)))

(defmacro ^:private with-read-location
  [loc mem-expr storage-backend f args]
  `(if (memory-reads? ~loc)
     ~mem-expr
     (apply ~f (some-> ~storage-backend storage/snapshot) ~args)))

(deftype Duratom
  [storage-backend underlying-atom ^Lock lock release read-from]

  IAtom2 ;; the new interface introduced in 1.9
  (swapVals [_ f]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [[o n :as result] (with-read-location
                               read-from
                               (swap-vals! underlying-atom f)
                               storage-backend
                               (juxt ut/identity f)
                               nil)]
        (when (not= o n)
          (storage/commit storage-backend n))
        result)))
  (swapVals [_ f arg1]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [[o n :as result] (with-read-location
                               read-from
                               (swap-vals! underlying-atom f arg1)
                               storage-backend
                               (juxt ut/identity f)
                               (list arg1))]
        (when (not= o n)
          (storage/commit storage-backend n))
        result)))
  (swapVals [o f arg1 arg2]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [[_ n :as result] (with-read-location
                               read-from
                               (swap-vals! underlying-atom f arg1 arg2)
                               storage-backend
                               (juxt ut/identity f)
                               (list arg1 arg2))]
        (when (not= o n)
          (storage/commit storage-backend n))
        result)))
  (swapVals [_ f arg1 arg2 more]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [[o n :as result] (with-read-location
                               read-from
                               (swap-vals! underlying-atom f arg1 arg2 more)
                               storage-backend
                               (juxt ut/identity f)
                               (list* arg1 arg2 more))]
        (when (not= o n)
          (storage/commit storage-backend n))
        result)))
  (resetVals [_ newval]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [[o n :as result] (with-read-location
                               read-from
                               (reset-vals! underlying-atom newval)
                               storage-backend
                               (juxt ut/identity (constantly newval))
                               nil)]
        (when (not= o n)
          (storage/commit storage-backend n))
        result)))

  IAtom
  (swap [_ f]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (with-read-location
                     read-from
                     (swap! underlying-atom f)
                     storage-backend
                     f
                     nil)]
        (storage/commit storage-backend result)
        result)))
  (swap [_ f arg]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (with-read-location
                     read-from
                     (swap! underlying-atom f arg)
                     storage-backend
                     f
                     (list arg))]
        (storage/commit storage-backend result)
        result)))
  (swap [_ f arg1 arg2]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (with-read-location
                     read-from
                     (swap! underlying-atom f arg1 arg2)
                     storage-backend
                     f (list arg1 arg2))]
        (storage/commit storage-backend result)
        result)))
  (swap [_ f arg1 arg2 more]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (with-read-location
                     read-from
                     (apply swap! underlying-atom f arg1 arg2 more)
                     storage-backend
                     f
                     (list* arg1 arg2 more))]
        (storage/commit storage-backend result)
        result)))
  (compareAndSet [_ oldval newval]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [commit? (with-read-location
                      read-from
                      (compare-and-set! underlying-atom oldval newval)
                      storage-backend
                      (partial = oldval)
                      nil)]
        (when commit?
          ;; commit the new value - NOT boolean (what `compare-and-set!` returned)
          (storage/commit storage-backend newval))
        commit?)))
  (reset [_ newval]
    (ut/assert-not-released! release)
    (ut/with-locking lock
      (let [result (with-read-location
                     read-from
                     (reset! underlying-atom newval)
                     nil
                     (constantly newval)
                     nil)]
        (storage/commit storage-backend result)
        result)))
  IRef ;; watches/validators/meta/deref works against the underlying atom
  (setValidator [_ validator]
    (.setValidator ^IRef underlying-atom validator))
  (getValidator [_]
    (.getValidator ^IRef underlying-atom))
  (addWatch [this watch-key watch-fn]
    (.addWatch ^IRef underlying-atom watch-key watch-fn)
    this)
  (removeWatch [this watch-key]
    (.removeWatch ^IRef underlying-atom watch-key)
    this)
  (getWatches [_]
    (.getWatches ^IRef underlying-atom))
  IDeref
  (deref [_]
    (with-read-location
      read-from
      (deref underlying-atom)
      storage-backend
      ut/identity
      nil))
  IObj
  (withMeta [_ meta-map]
    (Duratom. storage-backend (with-meta underlying-atom meta-map) ^Lock lock release read-from))
  IMeta
  (meta [_]
    (meta underlying-atom))
  IReference
  (resetMeta [_ meta-map]
    (reset-meta! underlying-atom meta-map))
  (alterMeta [_ f args]
    (alter-meta! underlying-atom f args))
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
   (->Duratom make-backend lock init commits :memory))
  ([make-backend lock init commits read-from]
   (->Duratom make-backend lock init commits read-from nil))
  ([make-backend lock init commits read-from handle-error]
   (assert (ut/lock? lock)
           "The <lock> provided is NOT a valid implementation of `java.util.concurrent.locks.Lock`!")
   (let [raw-atom (*atom-ctor* nil)
         async-commits? (async? commits)
         backend* (cond-> raw-atom
                          async-commits? (agent :error-mode :continue)
                          true           make-backend)
         backend (if async-commits?
                   ;; need to do this on a separate step
                   (add-agent-error-handler backend* handle-error)
                   (add-atom-error-handler backend* handle-error))
         duratom (Duratom. backend raw-atom lock (ut/releaser) read-from)
         storage-init (storage/snapshot backend)
         reset? (memory-reads? read-from)]

     (if (and (some? storage-init) ;; found stuff - sync it
              reset?)
       ;; reset the raw atom directly to avoid writing exactly what was read
       (reset! raw-atom storage-init)
       (when (some? init)
         ;; empty storage means we start off with <initial-value>
         (if reset?
           (reset! duratom (ut/->init init))
           (storage/commit backend init))))

     duratom)))

(defn map->Duratom
  [{:keys [make-backend lock init commit-mode read-from error-handler]}]
  (->Duratom make-backend lock init commit-mode read-from error-handler))


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
    Duratom (storage/snapshot (.-storage_backend ^Duratom dura))
    Agent (when-let [snap (some-> (meta dura) ::storage/snapshot)]
            (snap))))


(def default-file-rw
  {:read  ut/read-edn-object  ;; for nippy use `nippy/thaw-from-file`
   :write ut/write-edn-object ;; for nippy use `nippy/freeze-to-file`
   :read-from DEFAULT_READ_LOCATION
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
   (map->Duratom (merge rw
                   {:lock lock ;; allow for explicit nil
                    :init initial-value
                    :make-backend (partial storage/->FileBackend
                                           (doto (jio/file file-path)
                                             (.createNewFile))
                                           (:read rw)
                                           (:write rw))}))))


(def default-postgres-rw
  {:read  ut/read-edn-string             ;; for nippy use `nippy/thaw`
   :write (partial ut/pr-str-fully true) ;; for nippy use `nippy/freeze`
   :column-type :text                    ;; for nippy use :bytea
   :read-from DEFAULT_READ_LOCATION
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
   (map->Duratom (merge rw
                   {:lock lock
                    :init initial-value
                    :make-backend (partial storage/->PGSQLBackend
                                           db-config
                                           (if (ut/postgres-table-exists? db-config table-name)
                                             table-name
                                             (do (ut/create-dedicated-table! db-config table-name (:column-type rw))
                                                 table-name))
                                           row-id
                                           (:read rw)
                                           (:write rw))}))))

(def default-sqlite-rw
  {:read  ut/read-edn-string             ;; for nippy use `nippy/thaw`
   :write (partial ut/pr-str-fully true) ;; for nippy use `nippy/freeze`
   :column-type :text                    ;; for nippy use :bytea
   :read-from DEFAULT_READ_LOCATION
   :commit-mode DEFAULT_COMMIT_MODE} ;; technically not needed but leaving it for transparency
  )

(defn sqlite-atom
  "Creates and returns a SQLite-backed atom. If the location denoted by the combination of <db-config> and <table-name> exists,
   it is read and becomes the initial value. Otherwise, the initial value is <init> and the table <table-name> is updated."
  ([db-config table-name]
   (sqlite-atom db-config table-name 0 (ReentrantLock.) nil))
  ([db-config table-name row-id]
   (sqlite-atom db-config table-name row-id (ReentrantLock.) nil))
  ([db-config table-name row-id lock initial-value]
   (sqlite-atom db-config table-name row-id lock initial-value default-sqlite-rw))
  ([db-config table-name row-id lock initial-value rw]
   (map->Duratom (merge rw
                  {:lock lock
                   :init initial-value
                   :make-backend (partial storage/->SQLiteBackend
                                          db-config
                                          (if (ut/sqlite-table-exists? db-config table-name)
                                            table-name
                                            (do (ut/create-dedicated-table! db-config table-name (:column-type rw))
                                                table-name))
                                          row-id
                                          (:read rw)
                                          (:write rw))}))))

(def default-s3-rw
  ;; `edn/read` doesn't make use of the object size, so no reason to fetch it from S3 (we communicate that via metadata).
  ;; Contrast that with `ut/s3-bucket-bytes` which needs to copy the bytes from the S3 input-stream to some output-stream
  ;; (using an internal buffer). In such cases (e.g. nippy encoded bytes), knowing the object size means we can avoid copying entirely.
  {:read (with-meta
           (partial ut/read-edn-object readers/default) ;; this will be called with two args
           {:ignore-size? true}) ;; for nippy use `(comp nippy/thaw ut/s3-bucket-bytes)`
   :write      (partial ut/pr-str-fully true)  ;; for nippy use `nippy/freeze`
   :read-from DEFAULT_READ_LOCATION
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
   (map->Duratom (merge rw
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
                                           (:write rw))}))))

(def default-redis-rw
  {;; Redis library Carmine automatically uses Nippy for serialization/deserialization Clojure types
   ;; So by just replacing these functions with `identity` they will be serialized with Nippy
   :read  ut/read-edn-string
   :write (partial ut/pr-str-fully true)
   :read-from DEFAULT_READ_LOCATION
   :commit-mode DEFAULT_COMMIT_MODE}) ;; technically not needed but leaving it for transparency)

(defn redis-atom
  "Creates and returns a Redis-backed atom. If the location denoted by the combination of <db-config> and <key-name> exists,
  it is read and becomes the initial value. Otherwise, the initial value is <init> and the key <key-name> is updated."
  ([db-config key-name]
   (redis-atom db-config key-name (ReentrantLock.) nil))
  ([db-config key-name lock initial-value]
   (redis-atom db-config key-name lock initial-value default-redis-rw))
  ([db-config key-name lock initial-value rw]
   (map->Duratom (merge rw
                   {:lock lock
                    :init initial-value
                    :make-backend (partial storage/->RedisBackend
                                           db-config
                                           key-name
                                           (:read rw)
                                           (:write rw))}))))

(def default-fileio-rw
  {:read  ut/read-edn-object
   :write (partial ut/pr-str-fully true)
   :commit-mode DEFAULT_COMMIT_MODE})

(defn fileio-atom
  ([http-post! key-duratom]
   (fileio-atom http-post! key-duratom (ReentrantLock.) nil))
  ([http-post! key-duratom lock initial-value]
   (fileio-atom http-post! key-duratom lock initial-value default-fileio-rw))
  ([http-post! key-duratom lock initial-value rw]
   (map->Duratom (merge rw
                   {:lock lock
                    :init initial-value
                    :make-backend (partial storage/->FileIOBackend
                                           http-post!
                                           key-duratom
                                           (:read rw)
                                           (:write rw)
                                           (:expiry rw "2w"))})))) ;; two weeks by default


(defmulti duratom
  "Top level constructor function for the <Duratom> class.
   Built-in <backed-by> types are `:local-file`, `:postgres-db`, `:sqlite-db`, `:redis-db` & `:aws-s3`."
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

(defmethod duratom :sqlite-db
  [_ & {:keys [db-config table-name row-id init lock rw]
        :or {lock (ReentrantLock.)
             rw default-sqlite-rw}}]
  (sqlite-atom db-config table-name row-id lock init rw))

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

(defmethod duratom :file.io
  [_ & {:keys [http-post! key-duratom init lock rw]
        :or {lock (ReentrantLock.)
             rw default-fileio-rw}}]
  (fileio-atom http-post! key-duratom lock init rw))

;;=========================================================
;;=======================DURAGENT==========================

(defn- duragent*
  "Common constructor for duragents"
  [init meta-map ehandler make-backend]
  (let [release (ut/releaser)
        ag (agent nil
                  :error-mode :continue
                  :validator (fn [_]
                               ;; this can only be seen by
                               ;; the agent's error-handler
                               ;; NOT the caller of `send` (unlike atom `swap!`)
                               (ut/assert-not-released! release)
                               true))
        backend (with-meta (make-backend ag)
                           ;; force synchronous recommits
                           {:error-handler (fn [e] (throw e))})
        storage-init (storage/snapshot backend)
        cleanup-lock (ReentrantLock.)
        safe-to-add-watch? (promise)
        final-agent (delay
                      (-> ag
                          (add-watch ::storage/commit
                             (fn [_ _ _ n]
                               (try
                                 (storage/commit backend n)
                                 (catch Exception e
                                   (throw
                                     (ex-info (str "Commit error: " e)
                                              {:type ::storage/commit-error}
                                              e))))))
                          (doto
                            (reset-meta!
                              (merge meta-map
                                     {::storage/destroy
                                      (partial storage/safe-cleanup! backend release cleanup-lock)
                                      ::storage/snapshot
                                      #(storage/snapshot backend)}))
                            (set-error-handler!
                              (if (nil? ehandler)
                                ut/noop
                                (fn [a e]
                                  (if (= ::storage/commit-error (some-> (ex-data e) :type))
                                    (ehandler a (ex-cause e) #(storage/commit backend @a))
                                    (ehandler a e))))))))]
    (if (some? storage-init)
      ;; found stuff - sync it before adding the commit watch
      (send-off ag (fn [& _]
                     (future ;; make sure state has changed before adding commit watch
                       (while (nil? @ag)
                         (Thread/sleep 5))
                       (deliver safe-to-add-watch? true))
                     storage-init))
      ;; empty storage - trigger first commit
      (do (deliver safe-to-add-watch? true)
          (send-off @final-agent (constantly (ut/->init init)))))
    (and @safe-to-add-watch? ;; wait for storage syncing
         @final-agent)))

(defmulti duragent
          "duragent is to agent, what duratom is to atom"
          (fn [backed-by & _args]
            backed-by))

(defmethod duragent :local-file
  [_ & {:keys [file-path init rw meta]
        :or {rw default-file-rw}}]
  (let [make-backend (partial storage/->FileBackend
                              (doto (jio/file file-path)
                                (.createNewFile))
                              (:read rw)
                              (:write rw))]
    (duragent* init meta (:error-handler rw) make-backend)))

(defmethod duragent :postgres-db
  [_ & {:keys [db-config table-name row-id init lock rw meta]
        :or {lock (ReentrantLock.)
             rw default-postgres-rw}}]
  (let [make-backend (partial storage/->PGSQLBackend
                              db-config
                              (if (ut/postgres-table-exists? db-config table-name)
                                table-name
                                (do (ut/create-dedicated-table! db-config table-name (:column-type rw))
                                    table-name))
                              row-id
                              (:read rw)
                              (:write rw))]
    (duragent* init meta (:error-handler rw) make-backend)))

(defmethod duragent :sqlite-db
  [_ & {:keys [db-config table-name row-id init lock rw meta]
        :or {lock (ReentrantLock.)
             rw default-sqlite-rw}}]
  (let [make-backend (partial storage/->SQLiteBackend
                              db-config
                              (if (ut/sqlite-table-exists? db-config table-name)
                                table-name
                                (do (ut/create-dedicated-table! db-config table-name (:column-type rw))
                                    table-name))
                              row-id
                              (:read rw)
                              (:write rw))]
    (duragent* init meta (:error-handler rw) make-backend)))

(defmethod duragent :aws-s3
  [_ & {:keys [credentials bucket key init lock rw meta]
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
    (duragent* init meta (:error-handler rw) make-backend)))

(defmethod duragent :redis-db
  [_ & {:keys [db-config key-name init lock rw meta]
        :or {lock (ReentrantLock.)
             rw default-redis-rw}}]
  (let [make-backend (partial storage/->RedisBackend
                              db-config
                              key-name
                              (:read rw)
                              (:write rw))]
    (duragent* init meta (:error-handler rw) make-backend)))

(defmethod duragent :file.io
  [_ & {:keys [http-post! key-duratom init lock rw meta]
        :or {lock (ReentrantLock.)
             rw default-fileio-rw}}]
  (let [make-backend (partial storage/->FileIOBackend
                              http-post!
                              key-duratom
                              (:read rw)
                              (:write rw)
                              (:expiry rw "2w"))]
    (duragent* init meta (:error-handler rw) make-backend)))
