(ns bee-record.sql
  (:refer-clojure :exclude [:select])
  (:require [honeysql.core :as honey]
            [clojure.string :as str]
            [clojure.walk :as walk]))

(def quoting (atom :ansi))

(defn- as-sql-field [table field]
  (keyword (str table "." (str/replace (name field) #"-" "_"))))

(defn- as-namespaced-field [table field]
  (keyword (str table "/" (name field))))

(defn- as-table [model]
  (or (:table model) (-> model :from first)))

(defn- fields-to [model fields]
  (let [table (as-table model)
        to-field (if table
                   (fn [field] [(as-sql-field table field)
                                (as-namespaced-field table field)])
                   (fn [field] [(keyword (str/replace (name field) #"-" "_")) field]))]
    (mapv to-field fields)))

(defn model [{:keys [table pk fields] :as definition}]
  (assoc definition
         :from [(keyword table)]
         :select (fields-to definition fields)))

(defn to-sql [model]
  (honey/format model
                :quoting @quoting
                :allow-dashed-names? true))

(defn select [model fields]
  (assoc model :select (fields-to model fields)))

(defn select+ [model fields]
  (let [new-fields (fields-to model fields)]
    (update model :select #(vec (concat % new-fields)))))

(defn- normalize-field [model field]
  (let [namespaced? #(re-find #"/" (name %))
        tabled? #(re-find #"\." (name %))]
    (if (keyword? field)
      (cond-> field
              (namespaced? field) (str/replace #"/" ".")
              (not (tabled? field)) (->> (as-sql-field (as-table model))))
      field)))

(defn- normalize-fields [model [op & rest]]
  (->> rest
       (map #(normalize-field model %))
       (cons op)
       vec))

(defn- normalize-map [model comp-map]
  (let [norm-kv #(normalize-field model %)
        normalize #(vec (cons := (map norm-kv %)))]
    (->> comp-map
         (map normalize)
         (cons :and)
         vec)))

(defn where [model comparision]
  (let [norm-where (walk/prewalk #(cond->> %
                                           (or (list? %) (vector? %)) (normalize-fields model)
                                           (map? %) (normalize-map model))
                                 comparision)]
    (assoc model :where norm-where)))
