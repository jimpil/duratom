(ns duratom.not-found.s3)

(defn create-bucket [creds bucket-name]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn get-object [& args]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn put-object [& args]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn delete-object [& args]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn does-bucket-exist [& args]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn get-object-metadata [& args]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

