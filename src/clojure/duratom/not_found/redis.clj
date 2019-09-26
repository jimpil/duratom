(ns duratom.not-found.redis)

(defmacro wcar [conn-opts & args]
  `(throw (UnsupportedOperationException.
              "Redis backend requires that you have `com.taoensso/carmine` on your classpath...")))

(defn exists
  [key-name]
  (throw (UnsupportedOperationException.
           "Redis backend requires that you have `com.taoensso/carmine` on your classpath...")))

(defn get
  [key-name]
  (throw (UnsupportedOperationException.
           "Redis backend requires that you have `com.taoensso/carmine` on your classpath...")))

(defn set
  [key-name value]
  (throw (UnsupportedOperationException.
           "Redis backend requires that you have `com.taoensso/carmine` on your classpath...")))

(defn del
  [key-name]
  (throw (UnsupportedOperationException.
           "Redis backend requires that you have `com.taoensso/carmine` on your classpath...")))
