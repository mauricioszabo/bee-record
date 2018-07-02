(ns bee-record.queries-test
  (:require [bee-record.sql :as sql]
            [clojure.string :as str]
            [bee-record.test-helper :refer :all]
            [midje.sweet :refer :all]))

(declare accounts logins)
(def people (sql/model {:table :people
                        :pk :id
                        :fields [:id :name :age]
                        :associations {:accounts {:model (delay accounts)
                                                  :on {:id :user-id}}}}))

(def accounts (sql/model {:table :accounts
                          :pk :id
                          :fields [:id :account :user-id]
                          :associations {:person {:model people
                                                  :on {:user-id :id}}
                                         :logins {:model (delay logins)
                                                  :on {:id :account-id}}}}))
(def logins (sql/model {:table :logins
                        :pk :id
                        :fields [:login]}))

(fact "finds first record with specific primary key"
  (with-prepared-db
    (-> people (sql/find 1) (sql/query db))
    => {:people/id 1 :people/name "Foo" :people/age 10}

    (-> people (sql/where {:name "bar"}) (sql/find 1) (sql/query db))
    => nil))

(fact "eager-loads associations with LEFT JOIN"
  (with-prepared-db
    (-> people
        (sql/restrict '(:< :id 3))
        (sql/join :left {:accounts {:logins {:opts {:include-fields true}}}})
        (sql/query db))
    => [{:people/id 1 :people/name "Foo" :people/age 10 :logins/login "foo.tw"}
        {:people/id 1 :people/name "Foo" :people/age 10 :logins/login nil}
        {:people/id 2 :people/name "Bar" :people/age 20 :logins/login nil}]))
