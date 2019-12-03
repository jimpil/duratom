(ns duratom.fileio-test
  (:require [clojure.test :refer :all]
            [org.httpkit.client :as http]
            [duratom.core :as core]
            [clojure.java.io :as io]))

(defn- http-post
  [url ^String data]
  @(http/post url
              {:form-params {:text data}
               :headers {"Content-Type" "application/x-www-form-urlencoded"}}))

(deftest file.io-basic
  (testing "file.io "
    (with-open [dura (core/duratom :file.io
                                   :http-post! http-post
                                   :init {:x 1 :y 2}
                                   :key-duratom (core/duratom :local-file
                                                              :file-path "/tmp/fio.key"))]
      (Thread/sleep 2000)
      (swap! dura update :x inc)
      (is (every? (partial = 2) (vals @dura)))
      (Thread/sleep 2000))
    ;; clearing the file.io-duratom also clears the key-duratom
    (is (false? (.exists (io/file "/tmp/fio.key")))))
  )
