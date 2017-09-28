(ns duratom.core-test
  (:require [clojure.test :refer :all]
            [duratom.core :refer :all]
            [clojure.java.io :as jio]
            [duratom.utils :as ut]
            [clojure.edn :as edn]
            [taoensso.nippy :as nippy]))

(defn- common*
  [dura peek-in-source exists?]
  (-> dura
      (doto (swap! assoc :z 3))
      (doto (swap! dissoc :x)))

  (Thread/sleep 200)
  (is (= {:z 3 :y 2} @dura))
  (is (= {:z 3 :y 2} (peek-in-source)))

  (-> dura
      (doto (reset! [1 2 3]))
      (doto (swap!  (comp vec rest))))

  (Thread/sleep 200)
  (is (= [2 3] @dura))
  (is (= [2 3] (peek-in-source)))

  (destroy dura)
  (Thread/sleep 200)
  (is (= [2 3] @dura))
  (is (thrown? AssertionError (swap! dura conj 4)))
  (is (false? (exists?)) "Storage resource was NOT deleted!!!")
  )

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

    ;; empty file first
    (common* dura
             #(-> rel-path slurp read-string)
             #(.exists (jio/file rel-path)))
    ;; with-contents thereafter
    (spit rel-path (pr-str init))
    (common* (add-watch
               (duratom :local-file
                        :file-path rel-path
                        :init init)
               :log (fn [k r old-state new-state]
                      (println "Transitioning from" old-state "to" new-state "...")))
             #(-> rel-path slurp read-string)
             #(.exists (jio/file rel-path)))
    )
  )


(deftest postgres-backed-tests
  (println "PGSQL-backed atom...")
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

    ;; empty row first
    (common* dura
             #(ut/get-pgsql-value db-spec table-name 0 edn/read-string)
             #(some? (ut/get-pgsql-value db-spec table-name 0 edn/read-string)))
    ;; with-contents thereafter
    (ut/update-or-insert! db-spec table-name {:id 0 :value (pr-str init)} ["id = ?" 0])
    (common* (add-watch
               (duratom :postgres-db
                        :db-config db-spec
                        :table-name table-name
                        :row-id 0
                        :init init)
               :log (fn [k, r, old-state, new-state]
                      (println "Transitioning from" old-state "to" new-state "...")))
             #(ut/get-pgsql-value db-spec table-name 0 edn/read-string)
             #(some? (ut/get-pgsql-value db-spec table-name 0 edn/read-string)))
    )
  )

(deftest custom-rw-tests

  (testing "File-backed atom containing `nippy` bytes..."
    (let [rel-path "data_temp.txt"
          init {:x 1 :y 2}
          dura (add-watch
                 (duratom :local-file
                          :file-path rel-path
                          :init init
                          :rw {:read  nippy/thaw-from-file
                               :write nippy/freeze-to-file})
                 :log (fn [k r old-state new-state]
                        (println "Transitioning from" old-state "to" new-state "...")))]

      ;; empty file first
      (common* dura
               #(nippy/thaw-from-file rel-path)
               #(.exists (jio/file rel-path)))
      ;; with-contents thereafter
      (nippy/freeze-to-file rel-path init)
      (common* (add-watch
                 (duratom :local-file
                          :file-path rel-path
                          :rw {:read  nippy/thaw-from-file
                               :write nippy/freeze-to-file})
                 :log (fn [k r old-state new-state]
                        (println "Transitioning from" old-state "to" new-state "...")))
               #(nippy/thaw-from-file rel-path)
               #(.exists (jio/file rel-path)))

      )
    )

  (testing "PostgresDB-backed atom containing `nippy` bytes..."
    (let [db-spec {:classname   "org.postgresql.Driver"
                   :subprotocol "postgresql"
                   :subname     "//localhost:5432/atomDB"
                   :username    "dimitris"
                   :password    "secret"}
          table-name "atom_state_bytes"
          init {:x 1 :y 2}
          dura (add-watch
                 (duratom :postgres-db
                          :db-config db-spec
                          :table-name table-name
                          :row-id 0
                          :init init
                          :rw {:read  nippy/thaw
                               :write nippy/freeze
                               :column-type :bytea})
                 :log (fn [k, r, old-state, new-state]
                        (println "Transitioning from" old-state "to" new-state "...")))]

      ;; empty row first
      (common* dura
               #(ut/get-pgsql-value db-spec table-name 0 nippy/thaw)
               #(some? (ut/get-pgsql-value db-spec table-name 0 nippy/thaw)))
      ;; with-contents thereafter
      (ut/update-or-insert! db-spec table-name {:id 0 :value (nippy/freeze init)} ["id = ?" 0])
      (common* (add-watch
                 (duratom :postgres-db
                          :db-config db-spec
                          :table-name table-name
                          :row-id 0
                          :rw {:read  nippy/thaw
                               :write nippy/freeze
                               :column-type :bytea})
                 :log (fn [k, r, old-state, new-state]
                        (println "Transitioning from" old-state "to" new-state "...")))
               #(ut/get-pgsql-value db-spec table-name 0 nippy/thaw)
               #(some? (ut/get-pgsql-value db-spec table-name 0 nippy/thaw)))
      )
    )
  )

