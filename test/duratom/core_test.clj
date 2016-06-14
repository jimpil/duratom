(ns duratom.core-test
  (:require [clojure.test :refer :all]
            [duratom.core :refer :all]
            [clojure.java.io :as jio]
            [duratom.utils :as ut]))

(deftest file-backed-tests
  (println "File-backed atom...")
  (let [rel-path "data_temp.txt"
        init {:x 1 :y 2}
        dura (add-watch
               (duratom :local-file
                        :file-path rel-path
                        :init init)
               :log (fn [k r old-state new-state]
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


(deftest postgres-backed-tests
  (println "\nPGSQL-backed atom...")
  (let [db-spec {:classname   "org.postgresql.Driver"
                 :subprotocol "postgresql"
                 :subname     "//localhost:5432/atomDB"
                 :username    "dimitris"
                 :password    "secret"}
        table-name "atom_state"
        init {:x 1 :y 2}
        dura (add-watch
               (duratom :postgres-db
                        :db-config db-spec
                        :table-name table-name
                        :row-id 0
                        :init init)
               :log (fn [k, r, old-state, new-state]
                      (println "Transitioning from" old-state "to" new-state "...")))]
      (-> dura
          (doto (swap! assoc :z 3))
          (doto (swap! dissoc :x)))

      (Thread/sleep 200)
      (is (= {:z 3 :y 2} @dura))
      (is (= {:z 3 :y 2} (ut/get-pgsql-value db-spec table-name 0)))

      (-> dura
          (doto (reset! [1 2 3]))
          (doto (swap! (comp vec rest))))

      (Thread/sleep 200)
      (is (= [2 3] @dura))
      (is (= [2 3] (ut/get-pgsql-value db-spec table-name 0)))

      (destroy dura)

      (Thread/sleep 200)
      (is (= [2 3] @dura))
      (is (thrown? AssertionError (swap! dura conj 4)))
      (is (false? (ut/table-exists? db-spec table-name)) "Table was NOT deleted!!!")
    )
  )

