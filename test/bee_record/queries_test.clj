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
                                                  :on {:id :user-id}}}
                        :queries {:adults {:fn (fn [people age]
                                                 (sql/restrict people [:>= :age age]))}
                                  :same-initial {:fn (fn [people])}}}))


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

(fact "will apply function after query"
  (with-prepared-db
    (-> people
        (sql/restrict [:>= :id 2])
        (sql/map-results :people/name)
        (sql/query db))
    => ["Bar" "Baz"]))

; (fact "will query first model, then use results in another query"
;   (with-prepared-db
;     (-> people
;         (sql/restrict [:in :id [2 3]])
;         (sql/with-results (fn [results]
;                             (let)
;                             (map #(sql/where people [:> :id (:id %)])))))))

(fact "will return defined queries"
  (with-prepared-db
    (-> people
        (sql/restrict [:like :name "B%"])
        (sql/return :adults 17)
        (sql/query db))
    => [{:people/id 2 :people/name "Bar" :people/age 20}]))

(fact "will preload (with another query) defined queries")
