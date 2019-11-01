(ns duratom.core-test
  (:require [clojure.test :refer :all]
            [duratom.core :refer :all]
            [duratom.utils :as ut]
            [taoensso.nippy :as nippy]
            [clojure.java
             [shell :as shell]
             [io :as io]]
            [clojure.string :as str])
  (:import (clojure.lang Agent)
           (java.io StringWriter)))
;====================================
;; start the `default` VM:
;$ docker-machine start default

;; switch to `default` when using any docker commands
;$ eval $(docker-machine env default)

;; start up the specified containers
;$ docker-compose up -d

;; run tests
; lein test OR selectively in repl

;; shutdown the containers
;$ docker-compose down
;====================================

(defonce docker-default-machine-ip
  ;; `localhost` which works on Ubuntu simply won't work on MacOS.
  ;; we need the public IP of the docker-machine - `default` in this case.
  ;; this tends to be 192.168.99.100, but we can easily check via `docker-machine ip default`
  (delay
    (or (try
          (some-> (shell/sh "docker-machine" "ip" "default")
                  :out
                  str/trim-newline
                  not-empty)
          (catch Throwable _))
        ;; perhaps docker-machine is not involved
        "localhost")))

(defn- common*
  [dura exists? async?]
  (let [sleep-time 200
        [f atom?] (if (instance? Agent dura)
                    [send-off false]
                    [swap! true])]
    (-> dura ;; init = {:x 1 :y 2}
        (doto (f assoc :z 3))
        (doto (f dissoc :x)))

    (when async?
      (Thread/sleep sleep-time))

    (is (= {:z 3 :y 2} @dura))
    (is (= (backend-snapshot dura) @dura))

    (-> dura
        (doto (f  (constantly [1 2 3])))
        (doto (f  (comp vec rest))))

    (when async?
      (Thread/sleep sleep-time))

    (is (= [2 3] @dura))
    (is (= (backend-snapshot dura) @dura))

    (when async?
      (Thread/sleep sleep-time))

    (if atom?
      (is (= [[2 3] [1 2 3]]
             (reset-vals! dura [1 2 3])))
      ;; don't break the assertions below
      (f dura (constantly [1 2 3])))

    (when async?
      (Thread/sleep sleep-time))

    (if atom?
      (is (= [[1 2 3] [2 3]]
            (swap-vals! dura rest)))
      (f dura rest))

    (f dura (partial into (sorted-set)))

    (when async?
      (Thread/sleep sleep-time))

    (is (sorted? @dura))
    (is (= (sorted-set 2 3) (backend-snapshot dura) @dura))

    (f dura #(with-meta % {:a 1 :b 2}))

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
    (if atom?
      ;; this exception can only be seen/reacted to
      ;; inside the agent's error-handler
      (is (thrown? IllegalStateException (f dura conj 4)))
      (f dura conj 4)) ;; will trigger validator error
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

    ;; duragent version
    (when async?
      (let [p (promise)]
        (common* (duragent :local-file
                           :file-path rel-path
                           :init init
                           :rw (assoc default-file-rw
                                 :error-handler
                                 (fn [_ e] (deliver p e))))
                 #(.exists (io/file rel-path))
                 async?)
        (is (= "duratom/duragent has been released!"
               (.getMessage @p)))))
    )
  )


(deftest file-backed-tests
  (println "File-backed atom/agent with async commit...")
  (file-backed-tests* true)
  (println "File-backed atom with sync commit...")
  (file-backed-tests* false)
  )

(defn- postgres-backed-tests*
  [async?]
  (let [ip @docker-default-machine-ip
        db-spec {:classname   "org.postgresql.Driver"
                 :subprotocol "postgresql"
                 :subname     (str "//"  ip ":5432/atomDB") ;; localhost won't work on the mac
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

    ;; duragent version
    (when async?
      (common* (duragent :postgres-db
                         :db-config db-spec
                         :table-name table-name
                         :row-id 0
                         :init init)
               #(some? (ut/get-pgsql-value db-spec table-name 0 ut/read-edn-string))
               async?))
    )
  )


(deftest postgres-backed-tests
  (println "PGSQL-backed atom/agent with async commit...")
  (postgres-backed-tests* true)
  (println "PGSQL-backed atom with sync commit...")
  (postgres-backed-tests* false)
  )

(defn- redis-backed-tests*
  [async?]
  (let [ip @docker-default-machine-ip
        db-config  {:pool {}
                    :spec {:uri (str "redis://" ip ":6379/")}} ;; localhost won't work on the mac
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
    (common* dura key-exists? async?)
    ;; with contents
    (ut/redis-set db-config key-name (pr-str init))
    (common* (duratom :redis-db
                      :db-config db-config
                      :key-name key-name
                      :init init
                      :rw (cond-> default-redis-rw
                            (not async?) (assoc :commit-mode :sync)))
             key-exists?
             async?)

    ;; duragent version
    (when async?
      (common* (duragent :redis-db
                         :db-config db-config
                         :key-name key-name
                         :init init)
               key-exists?
               async?))

    ))

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
    (let [ip @docker-default-machine-ip
          db-spec {:classname   "org.postgresql.Driver"
                   :subprotocol "postgresql"
                   :subname     (str "//"  ip ":5432/atomDB")
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
    (let [ip @docker-default-machine-ip
          db-config  {:pool {}
                      :spec {:uri (str "redis://" ip ":6379/")}}
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

