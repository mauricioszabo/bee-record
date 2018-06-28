(ns bee-record.core)

(require '[honeysql.core :as sql]
         '[honeysql.helpers :as helpers])

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(sql/format {:select [[:a :tables/a] :b]
             :from [:tables]
             :pk :id
             :where [:= :a 10]}
            :quoting :ansi)
