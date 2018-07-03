(ns bee-record.aggregations-test
  (:require [bee-record.sql :as sql]
            [clojure.string :as str]
            [bee-record.test-helper :refer :all]
            [midje.sweet :refer :all]))

(def people
  (sql/model {:table :people
              :pk :id
              :fields [:id :name :age]
              ; :associations {:accounts {:model (delay accounts)
              ;                           :on {:id :user-id}}}
              :queries {:by-ids {:fn (fn [model]
                                       (sql/where people [:in :id (sql/select model [:id])]))
                                  :aggregation {:people/id :people/id}}
                        :same-ids {:fn (fn [model]
                                         (sql/with-results model
                                           #(sql/where people [:in :id (map :people/id %)])))
                                   :aggregation {:people/id :people/id}}}}))

(reset! sql/logging #(do (print (first %) " ") (prn (vec (rest %)))))
(fact "will preload (with another query) defined queries"
  (with-prepared-db
    (-> people
        (sql/select [:id :name])
        (sql/restrict [:like :name "F%"])
        (sql/with :by-ids)
        (sql/query db)))
  => [{:people/id 1 :people/name "Foo"
       :by-ids [{:people/id 1 :people/name "Foo" :people/age 10}]}])

(fact "will preload (with another query) defined queries that uses with-results"
  (with-prepared-db
    (-> people
        (sql/select [:id :name])
        (sql/restrict [:like :name "B%"])
        (sql/with :same-ids)
        (sql/query db)))
  => [{:people/id 2 :people/name "Bar"
       :same-ids [{:people/id 2 :people/name "Bar" :people/age 20}]}
      {:people/id 3 :people/name "Baz"
       :same-ids [{:people/id 3 :people/name "Baz" :people/age 15}]}])
