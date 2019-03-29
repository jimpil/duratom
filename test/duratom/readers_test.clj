(ns duratom.readers-test
  (:require [clojure.test :refer :all]
            [duratom.utils :as ut]
            [duratom.readers :refer [sorted-readers
                                     queue-reader
                                     meta-reader]]
            [clojure.tools.reader.edn :as edn])
  (:import (clojure.lang PersistentQueue)))

(defn- test-common
  [raw-data target-coll readers]
  (let [data (into target-coll raw-data)
        printed-data (pr-str data)
        read-back-data (edn/read-string {:readers readers}
                                        printed-data)]
    (is (= data read-back-data))
    (is (= (class data) (class read-back-data)))))


(deftest printers-readers-round-trip

  (testing "sorted-readers"

    (testing "sorted-maps"
      (test-common {1 :a 2 :b 3 :c} (sorted-map) sorted-readers))

    (testing "sorted-sets"
      (test-common [1 2 3] (sorted-set) sorted-readers))
    )

  (testing "queue-reader"
    (test-common [:a :b :c :d] PersistentQueue/EMPTY queue-reader)
    )

  (testing "meta-reader"
    (testing "metadata support via the #duratom/iobj tag"
      (let [original-meta {:whatever "foo"}
            original-vector (with-meta [1 2 3 4 5] original-meta)
            printed-wrapped (ut/pr-str-fully true original-vector)
            read-back (edn/read-string {:readers meta-reader} printed-wrapped)]
        (is (= read-back original-vector))
        (is (= (class read-back) (class original-vector)))
        (is (= (meta read-back) original-meta))
        )
      )
    )

  )
