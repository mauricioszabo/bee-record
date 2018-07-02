(ns bee-record.test-helper
  (:require [bee-record.sql :as sql]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [honeysql.core :as honey]))

(defn as-sql [model]
  (update (sql/to-sql model) 0 #(-> %
                                    (str/replace #"\"" "`")
                                    str/trim)))

(def db {:classname   "org.hsqldb.jdbcDriver"
         :subprotocol "hsqldb:mem:1"
         :subname "1"})

(defn with-prepared-db* [f]
  (jdbc/execute! db "CREATE TABLE IF NOT EXISTS \"people\" (
    \"id\" INTEGER IDENTITY PRIMARY KEY,
    \"name\" VARCHAR(255),
    \"age\" INTEGER
  )")

  (jdbc/execute! db "CREATE TABLE IF NOT EXISTS \"accounts\" (
    \"id\" INTEGER IDENTITY PRIMARY KEY,
    \"account\" VARCHAR(255),
    \"user_id\" INTEGER
  )")

  (jdbc/execute! db "CREATE TABLE IF NOT EXISTS \"logins\" (
    \"id\" INTEGER IDENTITY PRIMARY KEY,
    \"login\" VARCHAR(255),
    \"account_id\" INTEGER
  )")
  (jdbc/with-db-transaction [db db]
    (jdbc/db-set-rollback-only! db)
    (jdbc/execute! db (honey/format {:values [{:id 1 :name "Foo", :age 10}
                                              {:id 2 :name "Bar", :age 20}
                                              {:id 3 :name "Baz", :age 30}]
                                     :insert-into :people}
                                    :quoting :ansi))
    (jdbc/execute! db (honey/format {:values [{:id 1 :account "twitter", :user_id 1}
                                              {:id 2 :account "fb", :user_id 1}]
                                     :insert-into :accounts}
                                    :quoting :ansi))
    (jdbc/execute! db (honey/format {:values [{:id 1 :login "foo.tw", :account_id 1}]
                                     :insert-into :logins}
                                    :quoting :ansi))
    (f db)))

(defmacro with-prepared-db [ & body]
  `(with-prepared-db* (fn [~'db] ~@body)))
