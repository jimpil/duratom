(ns duratom.core
  "A durable atom: a glimmer reactive cell whose mutations are persisted through
  a pluggable `duratom.backends/StorageBackend`.

  Because it implements `glimmer.ratom/IReactiveCell`, a duratom works everywhere
  a glimmer atom does — @, reset!, swap!, cursor, reaction — while persisting
  every change to its backend."
  (:require [glimmer.ratom :as ratom]
            [duratom.backends :as backends]
            [duratom.utils :as ut]))

(defn- write!
  "Persist (f old) and make it the state, under the state's lock so the read,
  the write to the backend and the update are one step: two threads swapping at
  once cannot both start from the same old value. Watchers run after the lock is
  released. Returns the new value."
  [dura f]
  (let [state (:state dura)
        [changed? new]
        (locking state
          (let [old @state
                new (f old)]
            (if (= old new)
              [false new]
              (do (backends/persist! (:backend dura) new)
                  (reset! state new)
                  [true new]))))]
    (when changed?
      (ratom/-notify-watches! dura))
    new))

(defrecord Duratom [backend state watches]
  ratom/IReactiveCell
  (-value [_] @state)
  (-reset! [this v] (write! this (constantly v)) v)
  (-add-watch! [_ w] (swap! watches conj w))
  (-remove-watch! [_ w] (swap! watches disj w))
  (-notify-watches! [this] (doseq [w @watches] (w this)))
  ratom/IAtomicSwap
  (-swap! [this f] (write! this f)))

(defn- stored-state
  "[true v] when `backend` holds a value v (nil or false included), else nil."
  [backend]
  (let [v (backends/load-state backend)]
    (when (if (satisfies? backends/StoredValue backend)
            (backends/stored? backend)
            (some? v))
      [true v])))

(defn make-duratom
  "Build a durable atom over `backend` (any `duratom.backends/StorageBackend`).
  If the backend already holds a value it becomes the initial state; otherwise
  the atom starts at `init` and persists it."
  [backend init]
  (let [d (->Duratom backend
                     (clojure.core/atom nil)
                     (clojure.core/atom #{}))]
    (if-let [[_ stored] (stored-state backend)]
      (reset! (:state d) stored)
      (let [v (ut/->init init)]
        ;; Persist unconditionally: -reset! skips a write that leaves the state
        ;; unchanged, and the state starts at nil, so an init of nil would
        ;; otherwise never reach the backend.
        (backends/persist! backend v)
        (reset! (:state d) v)))
    d))

(defn file-atom
  "A durable atom persisted as EDN to `path` on the local filesystem."
  ([path] (file-atom path nil))
  ([path init] (make-duratom (backends/file-backend path) init)))

(defn sql-atom
  "A durable atom persisted to `table` over a jolt-lang/db `dbspec`
  (e.g. \"sqlite::memory:\" or a postgres uri)."
  ([dbspec table] (sql-atom dbspec table 0 nil))
  ([dbspec table init] (sql-atom dbspec table 0 init))
  ([dbspec table row-id init]
   (make-duratom (backends/sql-backend dbspec table row-id) init)))

(defn backend-snapshot
  "The value currently persisted in `dura`'s backend, read fresh."
  [dura]
  (backends/load-state (:backend dura)))

(defn destroy
  "Remove `dura`'s persisted value and release its backend resources."
  [dura]
  (backends/clear! (:backend dura))
  (backends/close-backend (:backend dura))
  nil)
