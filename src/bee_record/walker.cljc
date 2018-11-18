(ns bee-record.walker
  (:require [clojure.walk :as walk]
            [clojure.set :as set]))

(defn- table-name [entity]
  (let [from (:from entity)]
    (cond
      (vector? from) (let [from-part (first from)]
                       (if (vector? from-part) (second from-part) from-part)))))

(defn field->select [mapping field]
  (let [table-name (-> mapping
                       (get-in [:entities (keyword (namespace field))])
                       table-name
                       name)
        table-field (->> field name (str table-name ".") keyword)]
    [table-field field]))

(defn- entities-and-joins [ents-joins join-tree prev-entity entity]
  (let [[entities joins] ents-joins
        join-in-tree (get-in join-tree [prev-entity entity])]
    (if (keyword? join-in-tree)
      (-> ents-joins
           (entities-and-joins join-tree prev-entity join-in-tree)
           (entities-and-joins join-tree join-in-tree entity))
      [(conj entities entity) (apply conj joins join-in-tree)])))

(defn- prepare-joins [mapping query]
  (let [join-tree {:person {:pet [:pets [:= :pets.people_id :people.id]]
                            :record :pet}
                   :pet {:person [:people [:= :pets.people_id :people.id]]
                         :record [:medicalrecord [:= :pets.id :medicalrecord.pet_id]]}
                   :record {:pet [:pets [:= :pets.id :medicalrecord.pet_id]]
                            :person :pet}}]
    (loop [[prev-field field & rest] (:select query)
           entities #{(-> prev-field namespace keyword)}
           joins []]
      (if-let [entity (some-> field namespace keyword)]
        (if (contains? entities entity)
          (recur rest entities joins)
          (let [[es js] (entities-and-joins [entities joins]
                                            join-tree
                                            (-> prev-field namespace keyword)
                                            entity)]
            (recur (cons field rest) es js)))
        joins))))

(defn parse-query [mapping query]
  (let [{:keys [select]} query
        joins (prepare-joins mapping query)
        first-entity (->> select
                         (map #(get-in mapping [:entities (-> % namespace keyword)]))
                         (filter identity)
                         first)]
    (cond-> (assoc first-entity :select (map #(field->select mapping %) select))
            (not-empty joins) (assoc :join joins))))

(defn create-hierarchy [mapping]
  (let [{:keys [entities joins]} mapping]
    (->> joins
         (reduce (fn [[e1 e2]])))))
