(ns bee-record.sql-test
  (:require [bee-record.sql :as sql]
            [bee-record.test-helper :refer :all]
            [midje.sweet :refer :all]))

(fact "will generate a SQL given a honey struct"
  (as-sql {:select [:a]}) => ["SELECT `a`"])

(def users
  (sql/model {:table :users
              :pk :id
              :fields [:id :first-name]}))

(fact "will generate a default query given a model"
  (as-sql users)
  => [(str "SELECT `users`.`id` AS `users/id`, "
           "`users`.`first_name` AS `users/first-name` "
           "FROM `users`")])

(fact "will translate a table"
  (as-sql (sql/model {:table :my-users :pk :id :fields [:id]}))
  => [(str "SELECT `my_users`.`id` AS `my-users/id` "
           "FROM `my_users`")])

(facts "when generating single-table queries"
  (fact "will override SELECT"
    (-> users (sql/select [:name :age]) as-sql)
    => [(str "SELECT `users`.`name` AS `users/name`, "
             "`users`.`age` AS `users/age` "
             "FROM `users`")])

  (fact "will alias in SELECT fields"
    (-> users (sql/select [[:name :n]]) as-sql)
    => ["SELECT `name` AS `n` FROM `users`"])

  (fact "will add fields to SELECT"
    (-> users (sql/select+ [:name]) as-sql)
    => [(str "SELECT `users`.`id` AS `users/id`, "
             "`users`.`first_name` AS `users/first-name`, "
             "`users`.`name` AS `users/name` "
             "FROM `users`")])

  (fact "will add WHERE clauses"
    (-> users (sql/where '(:= :id 1)) as-sql)
    => (just [#"WHERE `users`\.`id` = ?" 1]))

  (fact "will concatenate WHERE clauses"
    (-> users
        (sql/restrict '(:= :id 1))
        (sql/restrict '(:= :id 2))
        as-sql)
    => (just [#"WHERE \(`users`\.`id` = \? AND `users`\.`id` = \?\)" 1 2]))

  (fact "will not pollute WHERE clauses"
    (-> users
        (sql/restrict '(:= :id 1))
        (sql/restrict '(:= :id 2))
        (sql/restrict '(:= :id 3))
        as-sql)
    => (just [#"WHERE \(`users`.`id`.*`users`.`id`.*`users`.`id` = \?\)" 1 2 3])))

(facts "about WHERE clauses"
  (tabular
   (fact "normalizes WHERE clauses"
     (-> users (sql/where ?where) as-sql first) => (contains ?result))
   ?where                               ?result
   [:= :name :foo]                      "`users`.`name` = `users`.`foo`"
   [:and [:= :name "boo"] [:= :id 10]]  "`users`.`name` = ? AND `users`.`id` = ?"
   '(:= :name :foo)                     "`users`.`name` = `users`.`foo`"
   {:name 10}                           "`users`.`name` = ?"
   {:foo/name 10}                       "(`foo`.`name` = ?)"
   {:foo.name 10}                       "(`foo`.`name` = ?)"
   [:in :id {:from [:b]
             :select [:id]}]            "(`users`.`id` in (SELECT `id` FROM `b`))"))
