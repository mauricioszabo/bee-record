(ns bee-record.walker
  (:require [clojure.set :as set]
            [clojure.walk :as walk])
  (:require [honeysql.types :refer [#?@(:cljs [SqlCall])]])
  #?(:clj (:import [honeysql.types SqlCall])))

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

(defn- table-for-entity-field [mapping field]
  (when (keyword? field)
    (some-> mapping
            (get-in [:entities (keyword (namespace field))])
            table-name
            name)))

(defn- field->select [mapping field]
  (let [get-field-part #(if (vector? %) (first %) %)]
    (cond
      (instance? SqlCall field)
      (update field :args #(field->select mapping %))

      (list? field)
      (map #(->> % (field->select mapping) get-field-part) field)

      (vector? field)
      [(->> field first (field->select mapping) get-field-part) (second field)]

      :else
      (if-let [table-name (table-for-entity-field mapping field)]
        [(->> field name (str table-name ".") keyword) field]
        field))))

(defn- entities-and-joins [ents-joins join-tree prev-entity entity]
  (let [[entities joins] ents-joins
        join-in-tree (get-in join-tree [prev-entity entity])]
    (if (keyword? join-in-tree)
      (-> ents-joins
          (entities-and-joins join-tree prev-entity join-in-tree)
          (entities-and-joins join-tree join-in-tree entity))
      [(conj entities entity) (apply conj joins join-in-tree)])))

(defn- all-fields [{:keys [where having group-by order-by]}]
  (->> where
       (tree-seq sequential? not-empty)
       (filter #(and (keyword? %) (namespace %)))))

(defn select-fields [part]
  (let [child-fn (fn [field]
                   (cond
                     (instance? SqlCall field) (:args field)
                     (vector? field) (take 1 field)
                     :else field))]

    (->> part
         seq
         (tree-seq #(and (coll? %) (not-empty %)) child-fn)
         (filter keyword?))))

(defn- prepare-joins [mapping query join-tree]
  (loop [[prev-field field & rest] (->> (all-fields query)
                                        (concat (select-fields (:order-by query)))
                                        (concat (select-fields (:group-by query)))
                                        (concat (select-fields (:select query)))
                                        (filter #(table-for-entity-field mapping %)))
         entities #{(some-> prev-field namespace keyword)}
         joins []]
    (let [entity (some-> field namespace keyword)]
      (cond
        (nil? field) joins

        (nil? entity) (throw (ex-info "Didn't find entity for field"
                                      {:field field
                                       :know-entities (-> mapping :entities keys)}))

        (contains? entities entity) (recur (cons field rest) entities joins)

        :else (let [[es js] (entities-and-joins [entities joins]
                                                join-tree
                                                (-> prev-field namespace keyword)
                                                entity)]
                (recur (cons field rest) es js))))))


(defn- field->where-field [mapping field]
  (if-let [table-name (table-for-entity-field mapping field)]
    (->> field name (str table-name ".") keyword)
    field))

(defn- prepare-fields [mapping where]
  (let [prepare (fn [field]
                  (if (sequential? field)
                    (prepare-fields mapping field)
                    (field->where-field mapping field)))]
    (mapv prepare where)))

(defn parser-for [mapping]
  (prn :WALKER)
  (let [join-tree (create-hierarchy mapping)]
    (fn [query]
      (prn :Q)
      (def mapping mapping)
      (def join-tree join-tree)
      (def query query)
      (let [{:keys [select where having group-by order-by]} query
            joins (prepare-joins mapping query join-tree)
            where (prepare-fields mapping where)
            having (prepare-fields mapping having)
            group (prepare-fields mapping group-by)
            order (prepare-fields mapping order-by)
            first-entity (some->> (select-fields (:select query))
                                  (filter #(table-for-entity-field mapping %))
                                  first
                                  (#(get-in mapping [:entities (-> % namespace keyword)])))
            honey (-> query
                      (merge first-entity)
                      (assoc :select (map #(field->select mapping %) select)))]
        (cond-> honey
                (not-empty where) (assoc :where where)
                (not-empty having) (assoc :having having)
                (not-empty group) (assoc :group-by group)
                (not-empty order) (assoc :order-by order)
                (not-empty joins) (assoc :join joins))))))

; ; #_
; (def m)
; (create-hierarchy
;  {:entities {:person {:from [:people]}
;              :child {:from [[:people :children]]}
;              :toy {:from [:toys]}
;              :preferred {:from [[:toys :preferred]]}
;              :house {:from [:houses]}}
;   :joins {[:house :person] [:= :people.house_id :houses.id]
;           [:person :child] [:= :children.parent_id :people.id]
;           [:child :toy] [:= :children.id :toys.child_id]
;           [:child :preferred] [:= :children.id :preferred.child_id]}})
;
; (clojure.repl/pst)
