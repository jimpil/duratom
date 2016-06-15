(ns duratom.not-found.s3)

(defn create-s3-bucket [creds bucket-name]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn get-value-from-s3 [creds bucket-name key]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn store-value-to-s3 [creds bucket key value]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn delete-object-from-s3 [credentials bucket-name k]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

(defn does-bucket-exist [creds bucket-name]
  (throw (UnsupportedOperationException.
           "S3 backend requires that you have `amazonica` on your classpath...")))

