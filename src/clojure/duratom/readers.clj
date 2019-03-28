(ns duratom.readers
  (:import (clojure.lang IPersistentMap IPersistentSet PersistentTreeSet PersistentTreeMap PersistentQueue IPersistentVector)
           (java.io Writer)))


;; Provide readers/printers for the built-in sorted collections (map & set)

(def sorted-readers
  "EDN reader extension for sorted-maps and sets"
  {'sorted/map (partial into (sorted-map))
   'sorted/set (partial into (sorted-set))})

(def ^:private print-methods
  (methods print-method))

(def ^:private map-print-method
  (get print-methods IPersistentMap))

(def ^:private vector-print-method
  (get print-methods IPersistentVector))

(def ^:private set-print-method
  (get print-methods IPersistentSet))

(defmethod print-method PersistentTreeMap ;; sorted-maps are instances of this
  [o ^Writer w]
  (.write w "#sorted/map ")
  (map-print-method o w))

(defmethod print-method PersistentTreeSet ;; sorted-sets are instances of this
  [o ^Writer w]
  (.write w "#sorted/set ")
  (set-print-method o w))

;; dummy wrapper object for IObj
(defrecord ObjectWithMeta [o])

(defmethod print-method ObjectWithMeta
  [o ^Writer w]
  (let [coll (:o o)]
    (.write w "#iobj ")
    (print-method [coll (meta coll)] w)))

(def meta-reader
  {'iobj (partial apply with-meta)})

(defmethod print-method PersistentQueue
  [o ^Writer w]
  (.write w "#queue ")
  (vector-print-method o w))

(def queue-reader
  {'queue (partial into PersistentQueue/EMPTY)})

(def default
  "The default set of readers used by duratom."
  {:readers (merge sorted-readers
                   meta-reader
                   queue-reader)})
