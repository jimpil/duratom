(ns duratom.error-handling-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [duratom.core :as core]
            [duratom.backends :as storage]
            [duratom.utils :as ut])
  (:import (java.util.concurrent.atomic AtomicLong)
           (java.io IOException)
           (java.nio.file Files)))

(defn- persist-exceptions-common*
  [duratom errors async?]
  (with-open [dura duratom]
    (with-redefs [storage/save-to-file!
                  (fn [_ _ _]
                    (throw (IllegalStateException. "whatever")))]
      (swap! dura update :x inc)
      (when async?
        (Thread/sleep 60))
      (swap! dura update :y inc)
      (is (= [2 3] ((juxt :x :y) @dura)))
      (when async?
        (Thread/sleep 60))
      (is (= 2 (count @errors)))
      (is (every? (partial instance? IllegalStateException) @errors)))))


(deftest persist-errors
  (testing "asynchronous error handler"
    (let [rel-path "data_temp1.txt"
          _ (when (.exists (io/file rel-path))
              (io/delete-file rel-path)) ;; proper cleanup before testing
          init {:x 1 :y 2}
          errors (atom [])]
      (persist-exceptions-common*
        (core/duratom :local-file
                      :file-path rel-path
                      :init init
                      :rw (assoc core/default-file-rw
                            :error-handler
                            (fn [e _]
                              (println "Error while saving asynchronously to file!")
                              (swap! errors conj e))))
        errors
        true)))


  (testing "synchronous error handler"
    (let [rel-path "data_temp2.txt"
          _ (when (.exists (io/file rel-path))
              (io/delete-file rel-path)) ;; proper cleanup before testing
          init {:x 1 :y 2}
          errors (atom [])]
      (persist-exceptions-common*
        (core/duratom :local-file
                      :file-path rel-path
                      :init init
                      :rw (assoc core/default-file-rw
                            :commit-mode :sync
                            :error-handler
                            (fn [e _]
                              (println "Error while saving synchronously to file!")
                              (swap! errors conj e))))
        errors
        false)))
  )

(defn- recommit-common
  [duratom async?]
  (let [errors (atom 0)]
    (with-open [dura duratom]
      (with-redefs [ut/move-file!
                    (let [al (AtomicLong. 0)]
                      (fn [s t]
                        ;; the guts of `move-file!`
                        (let [normal-flow #(Files/move (.toPath (io/file s))
                                                       (.toPath (io/file t))
                                                       ut/move-opts)
                              i (.incrementAndGet al)]
                          (case i
                            1 (do (swap! errors inc)
                                  (throw (IOException. "whatever")))
                            3 (do (swap! errors inc)
                                  (throw (IOException. "whatever")))
                            (normal-flow)))))]
        (swap! dura update :x inc) ;; fails once, but the error-handler recommits successfully
        (when async?
          (Thread/sleep 100))
        (swap! dura update :y inc) ;; fails once, but the error-handler recommits successfully
        (when async?
          (Thread/sleep 100))
        (is (= [2 3] ((juxt :x :y) @dura))) ;; as if nothing failed
        (is (= 2 @errors))))))


(deftest recommit-tests
  (testing "asynchronous recommits"
    (let [rel-path "data_temp3.txt"
          _ (when (.exists (io/file rel-path))
              (io/delete-file rel-path)) ;; proper cleanup before testing
          init {:x 1 :y 2}]
      (recommit-common
        (core/duratom :local-file
                      :file-path rel-path
                      :init init
                      :rw (assoc core/default-file-rw
                            :error-handler
                            (fn [_ recommit]
                              (println "Error while saving asynchronously to file! Recommitting...")
                              (recommit))))
        true)))

  (testing "synchronous recommits"
    (let [rel-path "data_temp4.txt"
          _ (when (.exists (io/file rel-path))
              (io/delete-file rel-path)) ;; proper cleanup before testing
          init {:x 1 :y 2}]
      (recommit-common
        (core/duratom :local-file
                      :file-path rel-path
                      :init init
                      :rw (assoc core/default-file-rw
                            :commit-mode :sync
                            :error-handler
                            (fn [_ recommit]
                              (println "Error while saving synchronously to file! Recommitting...")
                              (recommit))))
        false)))
  )
