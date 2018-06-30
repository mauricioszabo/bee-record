(ns bee-record.test-helper
  (:require [bee-record.sql :as sql]
            [clojure.string :as str]))

(reset! sql/quoting :mysql)

(defn as-sql [model]
  (update (sql/to-sql model) 0 str/trim))
