(ns bee-record.queries-test
  (:require [bee-record.sql :as sql]
            [clojure.string :as str]
            [bee-record.test-helper :refer :all]
            [midje.sweet :refer :all]))

(declare accounts logins)
(def people (sql/model {:table :people
                        :pk :id
                        :fields [:id :name :age]
                        :associations {:accounts {:model accounts
                                                  :on {:id :user-id}}}}))
(def accounts (sql/model {:table :accounts
                          :pk :id
                          :fields [:id :account :user-id]
                          :associations {:person {:model people
                                                  :on {:user-id :id}}
                                         :logins {:model logins
                                                  :on {:id :account-id}}}}))
(def logins (sql/model {:table :logins
                        :pk :id
                        :fields [:id :login :account-id]}))

(fact "finds first record with specific primary key"
  (with-prepared-db
    (-> people (sql/find 1) (sql/query db))
    => {:people/id 1 :people/name "Foo" :people/age 10}))
