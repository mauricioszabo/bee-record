(ns bee-record.aggregations-test
  (:require [bee-record.sql :as sql]
            [clojure.string :as str]
            [bee-record.test-helper :refer :all]
            [midje.sweet :refer :all]))

(def people
  (sql/model {:table :people
              :pk :id
              :fields [:id :name :age]
              :queries {:by-ids {:fn (fn [model]
                                       (sql/where people [:in :id (sql/select model [:id])]))
                                  :aggregation {:people/id :people/id}}
                        :same-ids {:fn (fn [model]
                                         (sql/with-results model
                                           #(-> people
                                                (sql/select [:id])
                                                (sql/where [:in :id (map :people/id %)]))))
                                   :aggregation {:people/id :people/id}}}}))

(reset! sql/logging #(println (first %) "\n" (vec (rest %))))
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
   (after :facts (reset! sql/logging nil))))

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

(def accounts (sql/model {:table :accounts
                          :pk :id
                          :fields [:id :account :user-id]
                          :associations {:person {:model people
                                                  :on {:user-id :id}}}}))

(facts "will generate a scope for associations"
  (fact "will allow us to query by model"
    (with-prepared-db
      (-> accounts
          (sql/return :join-person)
          (sql/query db)))
    => [{:people/id 1 :people/name "Foo" :people/age 10}])

  (fact "will allow us to preload by model"
    (with-prepared-db
      (-> accounts
          (sql/with :join-person)
          (sql/query db)))
    => [{:accounts/id 1 :accounts/account "twitter" :accounts/user-id 1
         :join-person [{:people/id 1 :people/name "Foo" :people/age 10}]}
        {:accounts/id 2 :accounts/account "fb" :accounts/user-id 1
         :join-person [{:people/id 1 :people/name "Foo" :people/age 10}]}])

  (fact "will allow us to query by results"
    (with-prepared-db
      (-> accounts
          (sql/return :person)
          (sql/query db)))
    => [{:people/id 1 :people/name "Foo" :people/age 10}])

  (fact "will allow us to preload by results"
    (with-prepared-db
      (-> accounts
          (sql/with :person)
          (sql/query db)))
    => [{:accounts/id 1 :accounts/account "twitter" :accounts/user-id 1
         :person [{:people/id 1 :people/name "Foo" :people/age 10}]}
        {:accounts/id 2 :accounts/account "fb" :accounts/user-id 1
         :person [{:people/id 1 :people/name "Foo" :people/age 10}]}]))
