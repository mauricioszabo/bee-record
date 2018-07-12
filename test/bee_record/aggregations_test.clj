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
                                           #(-> people
                                                (sql/select [:id])
                                                (sql/where [:in :id (map :people/id %)]))))
                                   :aggregation {:people/id :people/id}}}}))

(fact "will preload (with another query) defined queries"
  (with-prepared-db
    (-> people
        (sql/select [:id :name])
        (sql/restrict [:like :name "F%"])
        (sql/with :by-ids)
        (sql/query db)))
  => [{:people/id 1 :people/name "Foo"
       :by-ids [{:people/id 1 :people/name "Foo" :people/age 10}]}])

(def no-queries (atom 0))
(fact "will preload (with another query) defined queries that uses with-results"
  (with-prepared-db
    (-> people
        (sql/select [:id :name])
        (sql/restrict [:like :name "B%"])
        (sql/with :same-ids)
        (sql/query db)))
  => [{:people/id 2 :people/name "Bar"
       :same-ids [{:people/id 2}]}
      {:people/id 3 :people/name "Baz"
       :same-ids [{:people/id 3}]}]
  @no-queries => 2
  (background
   (before :facts (do
                    (reset! no-queries 0)
                    (reset! sql/logging (fn [query] (swap! no-queries inc)))))
   (after :facts (reset! sql/logging #(do (print "QUERY" (str/trim (first %)) " ") (prn (vec (rest %))))))))

(def people-more-scopes
  (assoc-in people [:queries :ids2] (-> people :queries :by-ids)))
(facts "when preloading with nesting"
  (fact "will preload arrays"
    (with-prepared-db
      (-> people-more-scopes
          (sql/select [:id :name])
          (sql/restrict [:like :name "F%"])
          (sql/with [:by-ids :same-ids :ids2])
          (sql/query db)))
    => [{:people/id 1 :people/name "Foo"
         :by-ids [{:people/id 1 :people/name "Foo" :people/age 10}]
         :same-ids [{:people/id 1}]
         :ids2 [{:people/id 1 :people/name "Foo" :people/age 10}]}])

  (fact "will preload maps (nested preload)"
    (with-prepared-db
      (-> people-more-scopes
          (sql/select [:id :age])
          (sql/where [:in :id [1 2]])
          (sql/with {:by-ids {:same-ids {}}})
          (sql/query db)))
    => [{:people/id 1 :people/age 10
         :by-ids [{:people/id 1 :people/name "Foo" :people/age 10
                   :same-ids [{:people/id 1}]}]}
        {:people/id 2 :people/age 20
         :by-ids [{:people/id 2 :people/name "Bar" :people/age 20
                   :same-ids [{:people/id 2}]}]}]))
