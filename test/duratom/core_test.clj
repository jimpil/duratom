(ns duratom.core-test
  (:require [clojure.test :refer :all]
            [duratom.core :refer :all]
            [clojure.java.io :as jio]))

(deftest file-backed-tests
  (let [rel-path "data_temp.txt"
        init {:x 1 :y 2}
        dura (add-watch
               (duratom :local-file
                        :file-path rel-path
                        :init init)
               :log (fn [k, r, old-state, new-state]
                      (println "Transitioning from" old-state "to" new-state "...")))]
    (-> dura
        (doto (swap! assoc :z 3))
        (doto (swap! dissoc :x)))
    (Thread/sleep 200)
    (is (= {:z 3 :y 2} @dura))
    (is (= {:z 3 :y 2} (-> rel-path slurp read-string)))

    (-> dura
        (doto (reset! [1 2 3]))
        (doto (swap!  (comp vec rest))))

    (Thread/sleep 200)

    (is (= [2 3] @dura))
    (is (= [2 3] (-> rel-path slurp read-string)))

    (destroy dura)
    (Thread/sleep 200)
    (is (= [2 3] @dura))
    (is (thrown? AssertionError (swap! dura conj 4)))
    (is (false? (.exists (jio/file rel-path))) "File was NOT deleted!!!")

    ))


