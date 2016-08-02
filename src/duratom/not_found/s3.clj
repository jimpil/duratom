(ns duratom.not-found.s3)

(defn create-bucket [creds bucket-name]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn get-object [creds bucket-name key]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn put-object [creds bucket key value]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn delete-object [credentials bucket-name k]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn does-bucket-exist [creds bucket-name]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

