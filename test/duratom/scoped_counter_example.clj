(ns duratom.scoped-counter-example
  (:require [duratom.core :as core]))


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
  [[start end :as scope] step dura]
  (if-let [v (peek @dura)]
    (do (swap! dura pop)
        v)
    (do
      (println dura  "has run out of values => wrapping round...")
      (reset! dura (scoped-seq start end step))
      (recur scope step dura))))

(defn demo [n-services [start end :as scope]]
  (let [services (mock-services n-services)
        service-ids (sort (map (comp str last :id) services))
        init-scoped-ranges (map #(scoped-seq % end n-services)
                                (take n-services (iterate inc start)))
        duratoms (map (fn [id init-range]
                        (core/duratom :postgres-db
                                      :db-config DB-SPEC
                                      :table-name "atom_state"
                                      :row-id (Long/parseLong id)
                                      :init init-range))
                      service-ids
                      init-scoped-ranges)
        generate (partial generate! scope n-services)]

    (dotimes [_ 15]
      (doall (pmap #(do (println (generate %))
                        (Thread/sleep (rand-int 1000)))
                   duratoms)))

    (doall (pmap core/destroy duratoms))

    )
  )
