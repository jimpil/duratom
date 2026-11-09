(ns duratom.utils
  "EDN serialization and file IO shared by the duratom backends."
  (:require [clojure.edn :as edn]
            [jolt.fs :as fs]))

(def edn-opts
  "EDN read options: return nil at end of input, so an empty file reads as nil."
  {:eof nil})

(defn pr-str-fully
  "`pr-str` with printing limits unbound, so large collections serialize whole."
  [& xs]
  (binding [*print-length* nil
            *print-level*  nil]
    (apply pr-str xs)))

(defn read-edn-string
  "Read one EDN value from string `s`; nil on empty input."
  [s]
  (edn/read-string edn-opts s))

(defn write-edn-file
  "Serialize `data` as EDN to `path` via a temp file + atomic rename."
  [path data]
  (let [tmp (str path ".tmp")]
    (spit tmp (pr-str-fully data))
    (fs/move tmp path {:replace-existing true})
    path))

(defn edn-file-present?
  "True when `path` exists and is non-empty, so it holds a value (nil included)."
  [path]
  (and (fs/exists? path) (pos? (fs/size path))))

(defn read-edn-file
  "Read one EDN value from `path`; nil when the file is absent or empty."
  [path]
  (when (edn-file-present? path)
    (read-edn-string (slurp path))))

(defn ->init
  "Resolve an initial-value spec: call a fn, force a delay, else return as-is."
  [x]
  (if (fn? x) (x) (force x)))
