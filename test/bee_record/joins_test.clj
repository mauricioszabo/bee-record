(ns bee-record.joins-test
  (:require [bee-record.sql :as sql]
            [bee-record.test-helper :refer :all]
            [midje.sweet :refer :all]))

(declare users)
(def accounts
  (sql/model {:table :accs
              :pk :id
              :fields [:id :login :pass]
              :associations {:users {:model (delay users)
                                     :on {:accs/user-id :users/id}}}}))

(def permissions
  (sql/model {:table :perms
              :pk :id
              :fields [:access]}))

(def roles
  (sql/model {:table :roles
              :pk :id
              :fields [:id :name]
              :associations {:perms {:model permissions
                                     :on {:roles/id :perms/role-id}}}}))

(def users
  (sql/model {:table :users
              :pk :id
              :fields [:id :name]
              :associations {:roles {:model roles
                                     :on {:users/id :roles/user-id}}
                             :accs {:model accounts
                                    :on {:users/id :accounts/user-id}}}}))

(facts "creating joins without associations"
  (fact "will left join"
    (-> users (sql/join :left :roles {:users/id :roles/user-id}) as-sql first)
    => #"LEFT JOIN `roles` ON \(`users`.`id` = `roles`.`user_id`\)")

  (fact "will right join"
    (-> users (sql/join :right :roles {:users/id :roles/user-id}) as-sql first)
    => #"RIGHT JOIN `roles` ON \(`users`.`id` = `roles`.`user_id`\)")

  (fact "will join"
    (-> users (sql/join :inner :roles {:users/id :roles/user-id}) as-sql first)
    => #"INNER JOIN `roles` ON \(`users`.`id` = `roles`.`user_id`\)")

  (fact "will join multiple tables"
    (-> users
        (sql/join :inner :roles {:users/id :roles/user-id})
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

  (fact "supports forward declarations"
    (-> accounts
        (sql/association-join :inner :users)
        as-sql first)
    => (contains "INNER JOIN `users` ON (`accs`.`user_id` = `users`.`id`)"))

  (fact "will join multiple associations"
    (-> users
        (sql/association-join :inner [:roles :accs])
        as-sql first)
    => #"INNER JOIN `roles`.*INNER JOIN `accs`")

  (fact "includes associated fields if we ask for it"
    (-> users
        (sql/association-join :inner {:roles {:opts {:include-fields true}}})
        as-sql first)
    => #"SELECT.*`roles`.`name`.*INNER JOIN `roles`")

  (fact "allows overriding the model"
    (-> users
        (sql/association-join :inner {:roles {:opts {:with-model
                                                     (sql/where roles {:name "admin"})}}})
        as-sql first)
    => #"INNER JOIN \(SELECT `roles`.`id`.*WHERE.*\) `roles` ON")

  (fact "create joins for nested associations"
    (-> users
        (sql/association-join :inner {:roles :perms})
        as-sql first)
    => (re-pattern (str "INNER JOIN `roles` "
                        "ON \\(`users`.`id` = `roles`.`user_id`\\).*"
                        "INNER JOIN `perms` "
                        "ON \\(`roles`.`id` = `perms`.`role_id`\\)")))

  (fact "create joins for nested associations with vectors"
    (-> users
        (sql/association-join :inner {:roles [:perms]})
        as-sql first)
    => (re-pattern (str "INNER JOIN `roles` "
                        "ON \\(`users`.`id` = `roles`.`user_id`\\).*"
                        "INNER JOIN `perms` "
                        "ON \\(`roles`.`id` = `perms`.`role_id`\\)")))

  (fact "create different joins for nested associations"
    (-> users
        (sql/association-join :inner {:roles {:opts {}
                                              :perms {:opts {:kind :left}}}})
        as-sql first)
    => (re-pattern (str "INNER JOIN `roles` "
                        "ON \\(`users`.`id` = `roles`.`user_id`\\).*"
                        "LEFT JOIN `perms` "
                        "ON \\(`roles`.`id` = `perms`.`role_id`\\)")))

  (fact "adds fields for nested associations"
    (-> users
        (sql/select [:name])
        (sql/association-join :inner {:roles {:perms {:opts {:include-fields true}}}})
        as-sql first)
    => #"SELECT `users`.`name` AS `users/name`, `perms`.`access` AS `perms/access`"))
