(ns bee-record.sql
  (:require [honeysql.core :as honey]
            [clojure.string :as str]))

(def quoting (atom :ansi))

(defn- fields-to [model fields]
  (let [table (or (:table model) (-> model :from first))
        to-field (if table
                   (fn [field] [(keyword (str table "." (str/replace (name field) #"-" "_")))
                                (keyword (str table "/" (name field)))])
                   (fn [field] [(keyword (str/replace (name field) #"-" "_")) field]))]
    (map to-field fields)))

(defn model [{:keys [table pk fields] :as definition}]
  (assoc definition
         :from [(keyword table)]
         :select (fields-to definition fields)))

(defn to-sql [model]
  (honey/format model
                :quoting @quoting
                :allow-dashed-names? true))
