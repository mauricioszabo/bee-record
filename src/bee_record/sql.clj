(ns bee-record.sql
  (:refer-clojure :exclude [select find distinct])
  (:require [honeysql.core :as honey]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [clojure.walk :as walk]))

(def quoting (atom :ansi))
(def logging (atom nil))

(defn- as-sql-field [table field]
  (keyword (str table "." (str/replace (name field) #"-" "_"))))

(defn- as-namespaced-field [table field]
  (keyword table (name field)))

(defn- as-table [model]
  (name (or (:table model) (-> model :from first))))

(defn- fields-to [model fields]
  (let [table (as-table model)
        to-field (if table
                   (fn [field] [(as-sql-field table field)
                                (as-namespaced-field table field)])
                   (fn [field] [(keyword (str/replace (name field) #"-" "_")) field]))]
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
                          (str/replace #"-" "_")
                          keyword)

    :else (as-sql-field (as-table model) field)))

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

(defn- assoc->query [model [k v]])
  ; (let [join-name (->> k name (str "join-") keyword)
  ;       p-name (as-table model)
  ;       a-name (as-table (-> model :associations k :model))
  ;       agg (-> v :aggregation (or (->> v
  ;                                       :on
  ;                                       (map (fn [[k v]] [(as-namespaced-field p-name k)
  ;                                                         (as-namespaced-field a-name v)]))
  ;                                       (into {}))))]
  ;   {join-name {:aggregation agg
  ;               :fn (fn [model]
  ;                     (-> model
  ;                         (join :inner k)
  ;                         (select (-> v :model :select))
  ;                         distinct))}}))

     ; k {:aggregation agg
     ;    :fn (fn [model]
     ;          (with-results model
     ;            (fn [results]
     ;              (let [assoc-model (get-in model [:associations k :model])
     ;                    conds (map (fn [[current agg]]
     ;                                 [:in agg (map current results)])
     ;                               agg)]
     ;
     ;                (where assoc-model (cons :and conds))))))}}))

(defn model [{:keys [table pk fields associations queries] :as definition}]
  (let [assoc-queries (->> associations
                           (map #(assoc->query definition %))
                           (into {}))]
    (assoc definition
           :from [table]
           :select (fields-to definition fields)
           :queries (merge queries assoc-queries))))
