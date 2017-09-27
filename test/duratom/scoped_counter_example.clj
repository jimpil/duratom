(ns duratom.scoped-counter-example
  (:require [duratom.core :as core]
            [clojure.test :refer :all]))


;; Requirement:
;==============
; A set of identifiable services need to generate unique, monotonically increasing, numeric ids within some range,
; and be able to survive a restart/crash at any given point in time. Sort of like a distributed counter.
; The mental burden of providing a large enough range (to avoid duplicates within some predefined period), falls on you!

(defn- mock-service [i]
  {:id (str "SERVICE_" i)})

(def DB-SPEC {:classname   "org.postgresql.Driver"
              :subprotocol "postgresql"
              :subname     "//localhost:5432/atomDB"
              :username    "dimitris"
              :password    "secret"})

(defn- mock-services [n]
  (->> n
       range
       (map (comp mock-service inc))))

(defn- scoped-seq [start end step]
  (->> step
       (range start end)
       reverse
       (into [])))

(defn generate!
  [end step dura]
  (if-let [v (peek @dura)]
    (do (swap! dura pop)
        v)
    (let [start (-> dura meta :service-id)]
      (println  (format "Service %s has run out of values => wrapping round..." start))
      (reset! dura (scoped-seq start end step))
      (recur end step dura))))

(defn demo [n-services [start end]]
  (let [services (mock-services n-services)
        service-ids (->> services
                         (map (comp str last :id))
                         sort)
        init-scoped-ranges (->> start
                                (iterate inc)
                                (take n-services)
                                (map #(scoped-seq % end n-services)))
        duratoms (map (fn [id init-range]
                        (let [num-id (Long/parseLong id)]
                          (-> (core/duratom :postgres-db
                                            :db-config DB-SPEC
                                            :table-name "atom_state"
                                            :row-id num-id
                                            :init init-range)
                              (with-meta {:service-id num-id}))))
                      service-ids
                      init-scoped-ranges)
        generate (partial generate! end n-services)]
    ;; unleash them all!
    (dotimes [_ 15]
      (doall (pmap #(do (Thread/sleep (rand-int 1000))
                        (println (generate %)))
                   duratoms)))

    (doall (pmap core/destroy duratoms))

    )
  )

#_(deftest demo-test
  (demo 10 [1 999999]))
