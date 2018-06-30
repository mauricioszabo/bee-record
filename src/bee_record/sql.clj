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
  (name (or (:table model) (-> model :from first))))

(defn- fields-to [model fields]
  (let [table (as-table model)
        to-field (if table
                   (fn [field] [(as-sql-field table field)
                                (as-namespaced-field table field)])
                   (fn [field] [(keyword (str/replace (name field) #"-" "_")) field]))]
    (mapv to-field fields)))

(defn model [{:keys [table pk fields] :as definition}]
  (assoc definition
         :from [table]
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
    (cond
      (not (keyword? field)) field

      (re-find #"\." (name field)) field

      (namespace field) (-> field
                            (str/replace #"/" ".")
                            (str/replace #"^:" "")
                            (str/replace #"-" "_")
                            keyword)

      :else (as-sql-field (as-table model) field)))

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

(defn- norm-conditions [model comparision]
  (walk/prewalk #(cond->> %
                          (or (list? %) (vector? %)) (normalize-fields model)
                          (map? %) (normalize-map model))
                comparision))

(defn where [model comparision]
  (assoc model :where (norm-conditions model comparision)))

(def ^:private joins {:inner :join
                      :left :left-join
                      :right :right-join})
(defn- normalize-join-map [table other conds]
  (->> conds
       (map (fn [[k v]] [(normalize-field {:table table} k)
                         (normalize-field {:table other} v)]))
       (into {})
       (norm-conditions {})))

(defn join [model kind foreign-table conditions]
  (let [join-model {(joins kind)
                    [foreign-table
                     (if (map? conditions)
                       (normalize-join-map (:table model) foreign-table conditions)
                       (norm-conditions model conditions))]}]
    (merge-with #(vec (concat %1 %2))
                model
                join-model)))
