(ns bee-record.sql
  (:refer-clojure :exclude [select find distinct])
  (:require [honeysql.core :as honey]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [clojure.walk :as walk]))

(def quoting (atom :ansi))
(def logging (atom nil))

(defn- as-sql-field [{:keys [translate-field translate-table] :as f} table field]
  (keyword (str (translate-table table) "." (translate-field (name field)))))

(defn- as-namespaced-field [table field]
  (keyword table (name field)))

(defn- as-table [model]
  (name (or (:table model) (-> model :from first))))

(defn- fields-to [model fields]
  (let [table (as-table model)
        {:keys [translate-field]} model
        to-field (if table
                   (fn [field] [(as-sql-field model table field)
                                (as-namespaced-field table field)])
                   (fn [field] [(keyword (translate-field (name field))) field]))]
    (mapv #(cond-> % (not (coll? %)) to-field) fields)))

(defn to-sql [model]
  (honey/format model
                :quoting @quoting
                :allow-dashed-names? true))

(defn select [model fields]
  (assoc model :select (fields-to model fields)))

(defn distinct [model]
  (update model :modifiers #(set (conj % "DISTINCT"))))

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
                          ((:translate-field model))
                          keyword)

    :else (as-sql-field model (as-table model) field)))

(defn- normalize-fields [model [op & rest]]
  (let [norm (->> rest
                  (map #(normalize-field model %))
                  (cons op)
                  vec)]
    (if (and (= op :in) (map? (last rest)))
      (update norm 2 with-meta {:normalized true})
      norm)))

(defn- normalize-map [model comp-map]
  (if (-> comp-map meta :normalized)
    comp-map
    (let [norm-kv #(normalize-field model %)
          normalize #(vec (cons := (map norm-kv %)))]
      (->> comp-map
           (map normalize)
           (cons :and)
           vec))))

(defn- norm-conditions [model comparision]
  (walk/prewalk #(cond->> %
                          (or (list? %) (vector? %)) (normalize-fields model)
                          (map? %) (normalize-map model))
                comparision))

(defn where [model comparision]
  (assoc model :where (norm-conditions model comparision)))

(defn restrict [model comparision]
  (let [old-where (:where model)
        new-where (norm-conditions model comparision)]
    (assoc model :where (cond
                          (nil? old-where) new-where
                          (-> old-where first (= :and)) (conj old-where new-where)
                          :else [:and old-where new-where]))))

(def ^:private joins {:inner :join
                      :left :left-join
                      :right :right-join})

(defn- merge-joins [m1 m2]
  (merge-with #(vec (concat %1 %2)) m1 m2))

(declare association-join)
(defn join
  "Joins one or more models. Kind is one of :inner, :left or :right to generate
an INNER, LEFT or RIGHT JOIN.

There are two ways of joining tables. First is to pass one
or multiple associations (in the associations parameters). The
second is to pass a foreign table, and conditions for the join.

Passing associations uses the following kind: you can join multiple associations
with the current model using a vector of associations:

(join people :inner :children) ; All people that have children
(join people :left [:children :dogs]) ; All people, with their children and dogs

If we want to \"nest associations\", we can pass a map:

; All people that have children, and their children have one or more dogs:
(join people :inner {:children :dogs})

We can pass options to the join too, using the format:
{:association {:opts {...}}}

Possible parameters are :with-model, :kind, and :include-fields.

; Will use model `adult-people` to join. This means that it'll change
; the join table, and quite possibly will join with a subselect
(join people :inner {:children {:opts {:with-model adult-people}}})

; This makes possible to use different kinds of joins in the model
; This query will bring all users, and zero or more of their children,
; but only if that children have dogs (LEFT JOIN, then INNER JOIN dogs):
(join people :left {:children {:dogs {:opts {:kind :inner}}}})

; Will include fields of associated join in the SELECT clause:
(join people :left {:children {:opts {:include-fields true}}})

Or, you can use the second way, but please notice that this means
that you'll have no support from BeeRecord to generate select
clauses, or to use different kinds of joins. You'll probably need
to write a bit more too:

(join people :inner :children {:people/id :children/parent-id})
; OR
(join people :inner :children [:= :people/id :children/parent-id])
"
  ([model kind associations] (association-join model kind associations))
  ([model kind foreign-table conditions]
   (let [ft-name (cond-> foreign-table (coll? foreign-table) last)
         join-model {(joins kind)
                     [foreign-table (norm-conditions model conditions)]}]
     (merge-joins model join-model))))

(defn- table-to-assoc-join [model]
  (let [complex-sql (or (-> model :from count (> 1))
                        (:where model)
                        (:join model)
                        (:group-by model)
                        (:having model)
                        (:union model)
                        (:union-all model))]
    (if complex-sql
      [model (:table model)]
      (:table model))))

(defn- get-assoc-model [model association opts]
  (let [primary-model (get-in model [:associations association :model])
        foreign-model (get opts :with-model primary-model)]
    (cond-> foreign-model (delay? foreign-model) deref)))

(defn- assoc-join [model kind association opts]
  (let [specs (get-in model [:associations association])
        foreign-model (get-assoc-model model association opts)
        kind (or (:kind opts) kind)]
    (when-not (or specs (map? foreign-model))
      (throw (ex-info "Invalid association" {:model model
                                             :association association
                                             :resolved-association foreign-model})))
    (cond-> model
            (:include-fields opts) (select+ (:select foreign-model))
            :always (join kind (table-to-assoc-join foreign-model) (:on specs)))))

(defn- map-join [model kind [assoc val]]
  (let [nested-assoc (dissoc val :opts)
        opts (:opts val)
        joined (assoc-join model kind assoc opts)
        nested-model (select (get-assoc-model model assoc opts) [])
        to-merge [:join :left-join :right-join :select]
        nested-joins (map (fn [[k v]] (select-keys
                                       (association-join nested-model kind {k v})
                                       to-merge))
                          nested-assoc)]
    (reduce merge-joins joined nested-joins)))

(defn association-join [model kind associations]
  (let [norm-map #(->> %
                       (map (fn [[k v]]
                              [k (cond
                                   (keyword? v) {v {:opts {}}}
                                   (map? v) v
                                   :else (->> v (map (fn [v] [v {}])) (into {})))]))
                       (into {}))]
    (cond
      (keyword? associations) (assoc-join model kind associations {})
      (map? associations) (reduce #(map-join %1 kind %2) model (norm-map associations))
      (coll? associations) (reduce #(assoc-join %1 kind %2 {}) model associations))))

(defn find
  ([model] (assoc model :limit 1 :resolve :first-only))
  ([model value]
   (-> model (restrict [:= (get model :pk :id) value])
       (assoc :limit 1
              :resolve :first-only))))

(defn query [model db]
  (let [map-res (:map-results model)
        with-res (:with-results model)
        after-query (:after-query model)]
    (when @logging (@logging (to-sql model)))
    (cond-> (jdbc/query db (to-sql model))
            map-res map-res
            (and with-res (not after-query)) (#(query (with-res %) db))
            after-query (after-query db)
            (-> model :resolve (= :first-only)) first)))

(defn map-results [model fun]
  (assoc model :map-results fun))

(defn with-results [model fun]
  (assoc model :with-results fun))

(defn return [model query-name & args]
  (let [scope-fn (-> model
                     (get-in [:queries query-name :fn])
                     (or (throw (ex-info "Invalid query for model"
                                         {:model model :query-name query-name}))))]
    (apply scope-fn model args)))

(defn- fns-for-aggregation [agg-fields]
  [(apply juxt (keys agg-fields))
   (apply juxt (vals agg-fields))])

(defn- aggregate [parents children query-name agg-fields]
  (let [[get-parent get-child] (fns-for-aggregation agg-fields)
        grouped (group-by get-child children)
        associate #(let [key-to-search (get-parent %)
                         children (get grouped key-to-search ())]
                     (assoc % query-name children))]
    (map associate parents)))

(declare with)
(defn- get-fields-for-with [model [query-name vals]]
  [query-name
   (with (return model query-name) vals)
   (or (get-in model [:queries query-name :aggregation])
       (throw (ex-info "This query can't be aggregated"
                       {:query-name query-name})))])

(defn- norm-with [obj]
  (cond
    (map? obj) (->> obj (map (fn [[k v]] [k (norm-with v)])) (into {}))
    (keyword? obj) {obj {}}
    (coll? obj) (->> obj (map #(vector % {})) (into {}))))

(defn with [model query-names]
  (let [get-fields (partial get-fields-for-with model)
        fields (mapv get-fields (norm-with query-names))]

    (assoc model :after-query
           (fn [parents db]
             (reduce (fn [results [query-name queried agg-fields]]
                       (let [with-res (:with-results queried)
                             children (if with-res
                                        (query (with-res parents) db)
                                        (query queried db))]
                         (aggregate results children query-name agg-fields)))
                     parents
                     fields)))))

(defn assoc->query-with-results [agg assoc-name]
  (fn [parent-model]
    (with-results parent-model
      (fn [results]
        (let [agg-model (get-assoc-model parent-model assoc-name {})
              aggregate-things (fn [result a [parent child]] (update a child conj (parent result)))
              mapped-results (loop [[result & rest] results
                                    acc {}]
                               (if result
                                 (recur rest (reduce (partial aggregate-things result) acc agg))
                                 acc))
              conds (->> mapped-results
                         (map #(vec (cons :in %)))
                         (cons :and))]
          (restrict agg-model conds))))))

(defn- assoc->query [model [k specs]]
  (let [join-name (->> k name (str "join-") keyword)
        agg (-> specs :aggregation (or (:on specs)))]
    {join-name {:aggregation agg
                :fn (fn [queried]
                      (-> queried
                          (join :inner k)
                          (select (:select (get-assoc-model model k {})))
                          distinct))}

     k {:aggregation agg
        :fn (assoc->query-with-results agg k)}}))

(defn model [{:keys [table pk fields associations queries] :as definition}]
  (let [assoc-queries (->> associations
                           (map #(assoc->query definition %))
                           (into {}))
        new-definition (assoc definition
                              :translate-table #(str/replace % #"-" "_")
                              :translate-field #(str/replace % #"-" "_"))]
    (assoc new-definition
           :select (fields-to new-definition fields)
           :queries (merge queries assoc-queries)
           :from [(keyword ((:translate-table new-definition) (name table)))])))
