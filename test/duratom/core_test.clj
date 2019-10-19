(ns duratom.core-test
  (:require [clojure.test :refer :all]
            [duratom.core :refer :all]
            [duratom.utils :as ut]
            [taoensso.nippy :as nippy]
            [clojure.java.io :as io])
  (:import (duratom.core Duratom)))

(defn- common*
  [^Duratom dura exists? async?]
  (let [sleep-time 200]
    (-> dura ;; init = {:x 1 :y 2}
        (doto (swap! assoc :z 3))
        (doto (swap! dissoc :x)))

    (when async?
      (Thread/sleep sleep-time))

    (is (= {:z 3 :y 2} @dura))
    (is (= (backend-snapshot dura) @dura))

    (-> dura
        (doto (reset! [1 2 3]))
        (doto (swap!  (comp vec rest))))

    (when async?
      (Thread/sleep sleep-time))

    (is (= [2 3] @dura))
    (is (= (backend-snapshot dura) @dura))

    (when async?
      (Thread/sleep sleep-time))

    (is (= [[2 3] [1 2 3]]
           (reset-vals! dura [1 2 3])))

    (when async?
      (Thread/sleep sleep-time))

    (is (= [[1 2 3] [2 3]]
           (swap-vals! dura rest)))

    (swap! dura (partial into (sorted-set)))

    (when async?
      (Thread/sleep sleep-time))

    (is (sorted? @dura))
    (is (= (sorted-set 2 3) (backend-snapshot dura) @dura))

    (swap! dura #(with-meta % {:a 1 :b 2}))

    (when async?
      (Thread/sleep sleep-time))

    (is (= (sorted-set 2 3) (backend-snapshot dura) @dura))
    (is (= {:a 1 :b 2} (meta (backend-snapshot dura)) (meta @dura)))

    (when async?
      (Thread/sleep sleep-time))

    (destroy dura)

    (when async?
      (Thread/sleep sleep-time))

    (is (sorted? @dura))
    (is (= #{2 3} @dura))
    (is (thrown? IllegalStateException (swap! dura conj 4)))
    (is (false? (exists?)) "Storage resource was NOT cleaned-up!!!")
    )
  )

(defn- file-backed-tests*
  [async?]
  (let [rel-path "data_temp.txt"
        _ (when (.exists (io/file rel-path))
            (io/delete-file rel-path)) ;; proper cleanup before testing
        init {:x 1 :y 2}
        dura (add-watch
               (duratom :local-file
                        :file-path rel-path
                        :init init
                        :rw (cond-> default-file-rw
                                    (not async?) (assoc :commit-mode :sync)))
               :log (fn [k r old-state new-state]
                      (println "Transitioning from" (ut/pr-str-fully true old-state)
                               "to" (ut/pr-str-fully true new-state) "...")))]

    ;; empty file first
    (common* dura #(.exists (io/file rel-path)) async?)
    ;; with-contents thereafter
    (spit rel-path (pr-str init))
    (common* (add-watch
               (duratom :local-file
                        :file-path rel-path
                        :init init
                        :rw (cond-> default-file-rw
                                    (not async?) (assoc :commit-mode :sync)))
               :log (fn [k r old-state new-state]
                      (println "Transitioning from" (ut/pr-str-fully true old-state)
                               "to" (ut/pr-str-fully true new-state) "...")))
             #(.exists (io/file rel-path))
             async?)
    )
  )


(deftest file-backed-tests
  (println "File-backed atom with async commit...")
  (file-backed-tests* true)
  (println "File-backed atom with sync commit...")
  (file-backed-tests* false)
  )

(defn- postgres-backed-tests*
  [async?]
  (let [db-spec {:classname   "org.postgresql.Driver"
                 :subprotocol "postgresql"
                 :subname     "//localhost:5432/atomDB"
                 :user        "dimitris"
                 :password    "secret"}
        table-name "atom_state"
        _ (ut/delete-relevant-row! db-spec table-name 0)
        init {:x 1 :y 2}
        dura (add-watch
               (duratom :postgres-db
                        :db-config db-spec
                        :table-name table-name
                        :row-id 0
                        :init init
                        :rw (cond-> default-postgres-rw
                                    (not async?) (assoc :commit-mode :sync)))
               :log (fn [k, r, old-state, new-state]
                      (println "Transitioning from" (ut/pr-str-fully true old-state)
                               "to" (ut/pr-str-fully true new-state) "...")))]

    ;; empty row first
    (common* dura
             #(some? (ut/get-pgsql-value db-spec table-name 0 ut/read-edn-string))
             async?)
    ;; with-contents thereafter
    (ut/update-or-insert! db-spec table-name {:id 0 :value (pr-str init)} ["id = ?" 0])
    (common* (add-watch
               (duratom :postgres-db
                        :db-config db-spec
                        :table-name table-name
                        :row-id 0
                        :init init
                        :rw (cond-> default-postgres-rw
                                    (not async?) (assoc :commit-mode :sync)))
               :log (fn [k, r, old-state, new-state]
                      (println "Transitioning from" (ut/pr-str-fully true old-state)
                               "to" (ut/pr-str-fully true new-state) "...")))
             #(some? (ut/get-pgsql-value db-spec table-name 0 ut/read-edn-string))
             async?)
    )
  )


(deftest postgres-backed-tests
  (println "PGSQL-backed atom with async commit...")
  (postgres-backed-tests* true)
  (println "PGSQL-backed atom with sync commit...")
  (postgres-backed-tests* false)
  )

(defn- redis-backed-tests*
  [async?]
  (let [db-config  {:pool {} :spec {:uri "redis://localhost:6379/"}}
        key-name "atom:state"
        init {:x 1 :y 2}
        key-exists? #(ut/redis-key-exists? db-config key-name)
        _ (ut/redis-del db-config key-name)
        dura (duratom :redis-db
                      :db-config db-config
                      :key-name key-name
                      :init init
                      :rw (cond-> default-redis-rw
                            (not async?) (assoc :commit-mode :sync)))]
    ;; empty key first
    (common* dura
             key-exists?
             async?)
    ;; with contents
    (ut/redis-set db-config key-name (pr-str init))
    (common* (duratom :redis-db
                      :db-config db-config
                      :key-name key-name
                      :init init
                      :rw (cond-> default-redis-rw
                            (not async?) (assoc :commit-mode :sync)))
             key-exists?
             async?)))

(deftest redis-backed-tests
  (println "Redis-backed atom with async commit...")
  (redis-backed-tests* true)
  (println "Redis-backed atom with sync commit...")
  (redis-backed-tests* false)
  )

(deftest custom-rw-tests

  (testing "File-backed atom containing `nippy` bytes..."
    (let [rel-path "data_temp.txt"
          _ (when (.exists (io/file rel-path))
              (io/delete-file rel-path)) ;; proper cleanup before testing
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
      (common* dura #(.exists (io/file rel-path)) true)
      ;; with-contents thereafter
      (nippy/freeze-to-file rel-path init)
      (common* (add-watch
                 (duratom :local-file
                          :file-path rel-path
                          :rw {:read  nippy/thaw-from-file
                               :write nippy/freeze-to-file})
                 :log (fn [k r old-state new-state]
                        (println "Transitioning from" old-state "to" new-state "...")))
               #(.exists (io/file rel-path))
               true)
      )
    )

  (testing "PostgresDB-backed atom containing `nippy` bytes..."
    (let [db-spec {:classname   "org.postgresql.Driver"
                   :subprotocol "postgresql"
                   :subname     "//localhost:5432/atomDB"
                   :user        "dimitris"
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
               #(some? (ut/get-pgsql-value db-spec table-name 0 nippy/thaw))
               true)
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
               #(some? (ut/get-pgsql-value db-spec table-name 0 nippy/thaw))
               true)
      )
    )

  (testing "Redis DB-backed atom containing `nippy` bytes..."
    (let [db-config  {:pool {} :spec {:uri "redis://localhost/"}}
          key-name "atom:state:bytes"
          key-exists? #(ut/redis-key-exists? db-config key-name)
          init {:x 1 :y 2}
          dura (duratom :redis-db
                        :db-config db-config
                        :key-name key-name
                        :init init
                        :rw {:read  identity
                             :write identity})]

      ;; empty row first
      (common* dura
               key-exists?
               true)
      ;; with-contents thereafter
      (ut/redis-set db-config key-name init)
      (common* (duratom :redis-db
                        :db-config db-config
                        :key-name key-name
                        :init init
                        :rw {:read  identity
                             :write identity})
               key-exists?
               true)
      )
    )
)

