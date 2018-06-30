(ns bee-record.sql-test
  (:require [bee-record.sql :as sql]
            [bee-record.test-helper :refer :all]
            [midje.sweet :refer :all]))

(def users
  (sql/model {:table :users
              :pk :id
              :fields [:id :name]
              :associations {:roles {:kind :many
                                     :fk :user_id}}}))

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
