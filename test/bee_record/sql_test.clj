(ns bee-record.sql-test
  (:require [bee-record.sql :as sql]
            [clojure.string :as str]
            [midje.sweet :refer :all]))

(reset! sql/quoting :mysql)
(fact "will generate a SQL given a honey struct"
  (sql/to-sql {:select [:a]}) => ["SELECT `a`"])

(def users
  (sql/model {:table "users"
              :pk :id
              :fields [:id :first-name]}))
(fact "will generate a default query given a model"
  (map str/trim (sql/to-sql users))
  => [(str "SELECT `users`.`id` AS `users/id`, "
           "`users`.`first_name` AS `users/first-name` "
           "FROM `users`")])
