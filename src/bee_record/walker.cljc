(ns bee-record.walker
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            ; [com.wsscode.pathom.core :as pathom]
            ; [com.wsscode.pathom.connect :as connect]
            [loom.graph :as graph]
            [loom.alg :as alg])
  (:require [honeysql.types :refer [#?@(:cljs [SqlCall])]]))
  ; #?(:clj (:import [honeysql.types SqlCall])))

(s/def ::entity (s/or
                 :alias keyword?
                 :table (s/cat :table any? :alias keyword?)))

(s/def ::entities (s/map-of keyword? ::entity))

(s/def ::join-tables (s/cat :first keyword? :second keyword?))
(s/def ::joins (s/map-of ::join-tables any?))
(s/def ::mapping (s/keys :req-un [::entities ::joins]))

(defn- walker-alias? [elem]
  (and (keyword? elem) (namespace elem)))

(defn- get-all-tables [query]
  (let [deep-flatten #(->> % (tree-seq coll? seq) (filter walker-alias?))
        fields (concat (-> query :select deep-flatten)
                       (->> query :where deep-flatten))
        tables (->> fields
                    (mapcat #(some-> %  walker-alias? keyword vector)))
        first-table (first tables)]
    {:first-table first-table
     :other-tables (-> tables set (disj first-table))}))

(defn- alias-for-table [mapping entity-name]
  (-> mapping :entities entity-name))

(defn- alias-for-attribute [mapping entity-name]
  (let [table-like (-> mapping :entities entity-name)]
    (if (keyword? table-like)
      (name table-like)
      (-> table-like peek name))))

(defn- norm-graphs [graphs new-graph]
  (let [graph-set (set new-graph)]
    (loop [should-add? true
           [fst & rst] graphs
           acc []]
      (let [f-set (set fst)]
        (prn :F-SET f-set :GRAPH graph-set :ADD? should-add?)
        (cond
          (and (nil? fst) should-add?) (conj acc new-graph)
          (nil? fst) acc
          (set/subset? f-set graph-set) (recur false rst (conj acc new-graph))
          (set/subset? graph-set f-set) (recur false rst (conj acc fst))
          :else (recur should-add? rst (conj acc fst)))))))

(defn- dedup-joins [joins]
  (loop [olds []
         [fst & rst] joins
         dedup []]
    (if (nil? fst)
      dedup
      (let [olds (->> olds
                      (mapcat #(->> % (reductions conj []) rest))
                      (sort-by #(- (count %))))
            prefix (->> olds
                        (filter #(->> fst (take (count %)) (= %)))
                        first)
            suffix (drop (count prefix) fst)]
        (recur (conj olds fst) rst (conj dedup suffix))))))

(defn- make-joins [mapping graph first-table tables]
  (let [{:keys [g joins]} graph
        clauses (reduce #(norm-graphs %1 (alg/shortest-path g first-table %2))
                        []
                        tables)]
    (->> clauses
         (map #(partition 2 1 %))
         dedup-joins
         (mapcat identity)
         (mapcat (fn [tables]
                   (prn :T-NAME
                        (->> tables second)
                        (->> tables second (alias-for-table mapping)))
                   [(->> tables second (alias-for-table mapping))
                    (or (get joins (set tables))
                        (throw (ex-info "Can't find join for tables"
                                        {:tables (set tables)})))]))
         vec)))

(defn- walk
  "Specific version of clojure.walk/walk"
  [inner outer form]
  (cond
    (list? form) (outer (apply list (map inner form)))
    (instance? clojure.lang.IMapEntry form)
    (outer (clojure.lang.MapEntry/create (inner (key form)) (inner (val form))))
    (seq? form) (outer (doall (map inner form)))
    (instance? clojure.lang.IRecord form)
    (outer (reduce (fn [r x] (conj r (inner x))) form form))

    (coll? form)
    (if (-> form meta :walker/keep)
      form
      (outer (into (empty form) (map inner form))))

    :else (outer form)))

(defn- postwalk [f form]
  (walk (partial postwalk f) f form))

(defn norm-where [mapping wheres]
  (postwalk (fn [elem]
              (if-let [table (walker-alias? elem)]
                (keyword (str (->> table keyword (alias-for-attribute mapping))
                              "."
                              (name elem)))
                elem))
            wheres))

(defn- norm-select [mapping select]
  (let [norm-attr #(if-let [table-from-q (walker-alias? %)]
                     (let [table (alias-for-attribute mapping (keyword table-from-q))
                           field (name %)]
                       [(keyword (str table "." field)) %])
                     %)
        norm-field #(if-let [table-from-q (walker-alias? %)]
                      (let [table (alias-for-attribute mapping (keyword table-from-q))
                            field (name %)]
                        (keyword (str table "." field)))
                      %)]
    (mapv #(if (keyword? %) (norm-attr %) (postwalk norm-field %))
          select)))


(defn parser-for [mapping]
  (s/assert* ::mapping mapping)
  (let [graph (reduce (fn [graph [[f1 f2] condition]]
                        (-> graph
                            (update :joins assoc (set [f1 f2]) condition)
                            (update :g graph/add-path f1 f2)))
                      {:g (graph/graph)}
                      (:joins mapping))]

    (fn [query]
      (let [{:keys [first-table other-tables]} (get-all-tables query)]
        (cond-> {:select (->> query :select (norm-select mapping))
                 :from [(-> mapping :entities first-table)]}

                (not-empty other-tables)
                (assoc :join (make-joins mapping graph first-table other-tables))

                (:where query)
                (assoc :where (norm-where mapping (:where query))))))))

; (def lol {::connect/sym 'bee-record.walker/foo
;           ::connect/resolve (fn [a b]
;                               {:foo/bar 20})
;           ::connect/input #{}
;           ::connect/output [:foo/bar]})
; (connect/defresolver foo
;   [foo bar]
;   {::connect/input #{}
;    ::connect/output [:foo/bar]}
;   {:foo/bar 10})
;
; ; (def x (atom 0))
; ; (repeatedly 20
; ;  (fn [] (future (swap! x inc)))) ;put atom in multiple threads with future
; ; (deref x)
; ;
; ; (dotimes [i 10] (.start (Thread. (fn [] (println i)))))
; ;
; ; (println "FOá")
; ;
; ; (let [p (gen-parser lol)]
; ;   (p {} [:foo/bar]))
; ;
; (defn- gen-parser [resolvers]
;   (pathom/parser
;     {::pathom/env {::pathom/reader [pathom/map-reader
;                                     connect/reader2
;                                     connect/open-ident-reader
;                                     pathom/env-placeholder-reader]
;                    ::pathom/placeholder-prefixes #{">"}}
;      ::pathom/mutate connect/mutate
;      ::pathom/plugins [(connect/connect-plugin {::connect/register resolvers})
;                        pathom/error-handler-plugin
;                        pathom/trace-plugin]}))
;
; (defn- get-query-db [])
;
; (defn- make-query-fn [eql query]
;   (let [{:keys [find where]} query
;         first-field (or (first find)
;                         (throw (ex-info "At least one field must be present on :find" {})))]
;     ,,,,
;     []))
;
; (defn parser-for' [mapping]
;   (let [{:keys [entities joins]} mapping]
;     ()))
;
; (s/def ::entities (s/map-of keyword? map?))
; (s/def ::joins (s/map-of set? coll?))
; ;                 :joins {#{:person :pet} [:= :pets.people_id :people.id]
; ;                       #{:pet :record} [:= :pets.id :medicalrecord.pet_id]}})
; ; )
; (s/conform ::joins {#{:foo :bar} []})
; (s/def ::mapping (s/keys :req-un [::entities ::joins]))
; #_
; (s/conform ::entities {:person {:from [:people]}
;                          :pet "pet"
;                          :record {:from [:medicalrecord]}})
;
; (defn- table-for [entity]
;   (let [from (:from entity)]
;     (cond
;       (keyword? from) from
;       (vector? from) (let [from-part (first from)]
;                        (if (vector? from-part) (second from-part) from-part)))))
;
; (defn- complete-hierarchy [orig-hierarchy all-keys]
;   (loop [hierarchy orig-hierarchy
;          [key & ks] all-keys]
;     (if key
;       (let [map (key hierarchy)
;             my-keys (keys map)
;             to-complete (set/difference all-keys (keys map) #{key})
;             completions (for [my-key my-keys
;                               new-key to-complete
;                               :let [val (get-in orig-hierarchy [my-key new-key])]
;                               :when val]
;                           [new-key my-key])
;             new-hierarchy (update hierarchy key merge (into {} completions))]
;
;         (cond
;           (-> completions count (= (count to-complete)))
;           (recur new-hierarchy ks)
;           ;
;           (-> completions count zero?)
;           (throw (ex-info "Join graph does not compute" {:entity key
;                                                          :missing-associations to-complete}))
;
;           :else
;           (recur new-hierarchy (cons key ks))))
;       hierarchy)))
;
; (defn- create-hierarchy [mapping]
;   (let [entity-for (fn [e-name] (-> mapping (get-in [:entities e-name]) table-for))
;         aggregate (fn [joins [e1 e2] conditions]
;                     (-> joins
;                         (assoc-in [e1 e2] [(entity-for e2) conditions])
;                         (assoc-in [e2 e1] [(entity-for e1) conditions])))
;         orig-hierarchy (reduce-kv aggregate {} (:joins mapping))]
;     (complete-hierarchy orig-hierarchy (set (keys orig-hierarchy)))))
;
; (defn- table-name [entity]
;   (let [from (:from entity)]
;     (cond
;       (vector? from) (let [from-part (first from)]
;                        (if (vector? from-part) (second from-part) from-part)))))
;
; (defn- table-for-entity-field [mapping field]
;   (when (keyword? field)
;     (some-> mapping
;             (get-in [:entities (keyword (namespace field))])
;             table-name
;             name)))
;
; (defn- field->select [mapping field]
;   (let [get-field-part #(if (vector? %) (first %) %)]
;     (cond
;       (instance? SqlCall field)
;       (update field :args #(field->select mapping %))
;
;       (list? field)
;       (map #(->> % (field->select mapping) get-field-part) field)
;
;       (vector? field)
;       [(->> field first (field->select mapping) get-field-part) (second field)]
;
;       :else
;       (if-let [table-name (table-for-entity-field mapping field)]
;         [(->> field name (str table-name ".") keyword) field]
;         field))))
;
; (defn- entities-and-joins [ents-joins join-tree prev-entity entity]
;   (let [[entities joins] ents-joins
;         join-in-tree (get-in join-tree [prev-entity entity])]
;     (if (keyword? join-in-tree)
;       (-> ents-joins
;           (entities-and-joins join-tree prev-entity join-in-tree)
;           (entities-and-joins join-tree join-in-tree entity))
;       [(conj entities entity) (apply conj joins join-in-tree)])))
;
; (defn- all-fields [{:keys [where having group-by order-by]}]
;   (->> where
;        (tree-seq sequential? not-empty)
;        (filter #(and (keyword? %) (namespace %)))))
;
; (defn select-fields [part]
;   (let [child-fn (fn [field]
;                    (cond
;                      (instance? SqlCall field) (:args field)
;                      (vector? field) (take 1 field)
;                      :else field))]
;
;     (->> part
;          seq
;          (tree-seq #(and (coll? %) (not-empty %)) child-fn)
;          (filter keyword?))))
;
; (defn- prepare-joins [mapping query join-tree]
;   (loop [[prev-field field & rest] (->> (all-fields query)
;                                         (concat (select-fields (:order-by query)))
;                                         (concat (select-fields (:group-by query)))
;                                         (concat (select-fields (:select query)))
;                                         (filter #(table-for-entity-field mapping %)))
;          entities #{(some-> prev-field namespace keyword)}
;          joins []]
;     (let [entity (some-> field namespace keyword)]
;       (cond
;         (nil? field) joins
;
;         (nil? entity) (throw (ex-info "Didn't find entity for field"
;                                       {:field field
;                                        :know-entities (-> mapping :entities keys)}))
;
;         (contains? entities entity) (recur (cons field rest) entities joins)
;
;         :else (let [[es js] (entities-and-joins [entities joins]
;                                                 join-tree
;                                                 (-> prev-field namespace keyword)
;                                                 entity)]
;                 (recur (cons field rest) es js))))))
;
;
; (defn- field->where-field [mapping field]
;   (if-let [table-name (table-for-entity-field mapping field)]
;     (->> field name (str table-name ".") keyword)
;     field))
;
; (defn- prepare-fields [mapping where]
;   (let [prepare (fn [field]
;                   (if (sequential? field)
;                     (prepare-fields mapping field)
;                     (field->where-field mapping field)))]
;     (mapv prepare where)))
;
; #_
; (defn parser-for [mapping]
;   (prn :WALKER)
;   (let [join-tree (create-hierarchy mapping)]
;     (fn [query]
;       (prn :Q)
;       (def mapping mapping)
;       (def join-tree join-tree)
;       (def query query)
;       (let [{:keys [select where having group-by order-by]} query
;             joins (prepare-joins mapping query join-tree)
;             where (prepare-fields mapping where)
;             having (prepare-fields mapping having)
;             group (prepare-fields mapping group-by)
;             order (prepare-fields mapping order-by)
;             first-entity (some->> (select-fields (:select query))
;                                   (filter #(table-for-entity-field mapping %))
;                                   first
;                                   (#(get-in mapping [:entities (-> % namespace keyword)])))
;             honey (-> query
;                       (merge first-entity)
;                       (assoc :select (map #(field->select mapping %) select)))]
;         (cond-> honey
;                 (not-empty where) (assoc :where where)
;                 (not-empty having) (assoc :having having)
;                 (not-empty group) (assoc :group-by group)
;                 (not-empty order) (assoc :order-by order)
;                 (not-empty joins) (assoc :join joins))))))
;
; ; ; #_
; ; (def m)
; ; (create-hierarchy
; ;  {:entities {:person {:from [:people]}
; ;              :child {:from [[:people :children]]}
; ;              :toy {:from [:toys]}
; ;              :preferred {:from [[:toys :preferred]]}
; ;              :house {:from [:houses]}}
; ;   :joins {[:house :person] [:= :people.house_id :houses.id]
; ;           [:person :child] [:= :children.parent_id :people.id]
; ;           [:child :toy] [:= :children.id :toys.child_id]
; ;           [:child :preferred] [:= :children.id :preferred.child_id]}})
; ;
; ; (clojure.repl/pst)
