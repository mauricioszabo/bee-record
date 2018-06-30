(ns bee-record.joins-test
  (:require [bee-record.sql :as sql]
            [bee-record.test-helper :refer :all]
            [midje.sweet :refer :all]))

(def roles
  (sql/model {:table :roles
              :pk :id
              :fields [:id :name]}))

(def users
  (sql/model {:table :users
              :pk :id
              :fields [:id :name]
              :associations {:roles {:model roles
                                     :on {:id :user-id}}}}))

(facts "creating joins without associations"
  (fact "will left join"
    (-> users (sql/join :left :roles {:id :user-id}) as-sql first)
    => #"LEFT JOIN `roles` ON \(`users`.`id` = `roles`.`user_id`\)")

  (fact "will right join"
    (-> users (sql/join :right :roles {:id :user-id}) as-sql first)
    => #"RIGHT JOIN `roles` ON \(`users`.`id` = `roles`.`user_id`\)")

  (fact "will join"
    (-> users (sql/join :inner :roles {:id :user-id}) as-sql first)
    => #"INNER JOIN `roles` ON \(`users`.`id` = `roles`.`user_id`\)")

  (fact "will join multiple tables"
    (-> users
        (sql/join :inner :roles {:id :user-id})
        (sql/join :inner :perms {:perms/role-id :roles/id})
        as-sql first)
    => (contains (str "INNER JOIN `roles` ON (`users`.`id` = `roles`.`user_id`) "
                      "INNER JOIN `perms` ON (`perms`.`role_id` = `roles`.`id`)"))))

(facts "creating joins from associations"
  (fact "generates a join"
    (-> users
        (sql/association-join :inner :roles)
        as-sql first)
    => (contains "INNER JOIN `roles` ON (`users`.`id` = `roles`.`user_id`)"))

  (fact "includes associated fields if we ask for it"
    (-> users
        (sql/association-join :inner :roles {:include-fields true})
        as-sql first)
    => #"SELECT.*`roles`.`name`.*INNER JOIN `roles`")

  (fact "allows overriding the model"
    (-> users
        (sql/association-join :inner :roles
                              {:with-model (sql/where roles {:name "admin"})})
        as-sql first)
    => #"INNER JOIN \(SELECT `roles`.`id`.*WHERE.*\) `roles` ON"))
