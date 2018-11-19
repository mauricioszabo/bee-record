(ns bee-record.walker
  (:require [clojure.set :as set]))

(defn- table-for [entity]
  (let [from (:from entity)]
    (cond
      (keyword? from) from
      (vector? from) (let [from-part (first from)]
                       (if (vector? from-part) (second from-part) from-part)))))

(defn- complete-hierarchy [orig-hierarchy all-keys]
  (loop [hierarchy orig-hierarchy
         [key & ks] all-keys]
    (if key
      (let [map (key hierarchy)
            my-keys (keys map)
            to-complete (set/difference all-keys (keys map) #{key})
            completions (for [my-key my-keys
                              new-key to-complete
                              :let [val (get-in orig-hierarchy [my-key new-key])]
                              :when val]
                          [new-key my-key])
            new-hierarchy (update hierarchy key merge (into {} completions))]

        (cond
          (-> completions count (= (count to-complete)))
          (recur new-hierarchy ks)
          ;
          (-> completions count zero?)
          (throw (ex-info "Join graph does not compute" {:entity key
                                                         :missing-associations to-complete}))

          :else
          (recur new-hierarchy (cons key ks))))
      hierarchy)))

(defn- create-hierarchy [mapping]
  (let [entity-for (fn [e-name] (-> mapping (get-in [:entities e-name]) table-for))
        aggregate (fn [joins [e1 e2] conditions]
                    (-> joins
                        (assoc-in [e1 e2] [(entity-for e2) conditions])
                        (assoc-in [e2 e1] [(entity-for e1) conditions])))
        orig-hierarchy (reduce-kv aggregate {} (:joins mapping))]
    (complete-hierarchy orig-hierarchy (set (keys orig-hierarchy)))))

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

(defn- prepare-joins [mapping query join-tree]
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
      joins)))

(defn parse-query [mapping query]
  (let [{:keys [select]} query
        joins (prepare-joins mapping query (create-hierarchy mapping))
        first-entity (->> select
                         (map #(get-in mapping [:entities (-> % namespace keyword)]))
                         (filter identity)
                         first)]
    (cond-> (assoc first-entity :select (map #(field->select mapping %) select))
            (not-empty joins) (assoc :join joins))))

(defn parser-for [mapping]
  (let [join-tree (create-hierarchy mapping)]
    (fn [query]
      (let [{:keys [select]} query
            joins (prepare-joins mapping query join-tree)
            first-entity (->> select
                              (map #(get-in mapping [:entities (-> % namespace keyword)]))
                              (filter identity)
                              first)]
        (cond-> (assoc first-entity :select (map #(field->select mapping %) select))
                (not-empty joins) (assoc :join joins))))))
