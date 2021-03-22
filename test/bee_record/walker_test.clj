(ns bee-record.walker-test
  (:require [clojure.test :refer [deftest testing is]]
            [check.core :refer [check]]
            [bee-record.walker :as walker]
            [matcher-combinators.test]
            [matcher-combinators.matchers :refer [in-any-order]]))

(def mapping {:entities {:person :people
                         :pet :pets
                         :record :medicalrecord}
              :joins {[:person :pet] [:= :pets.people_id :people.id]
                      [:pet :record] [:= :pets.id :medicalrecord.pet_id]}})

(def q (walker/parser-for mapping))

(deftest basic-queries
  (check (q {:select [:person/id :person/name]})
         => {:select [[:people.id :person/id] [:people.name :person/name]]
             :from [:people]}))

(def parse (walker/parser-for mapping))

(deftest generation-of-a-honey-query
  (testing "generation of a query with a direct join"
    (check (parse {:select [:person/name :pet/color :pet/race]})
           => {:select [[:people.name :person/name]
                        [:pets.color :pet/color]
                        [:pets.race :pet/race]]
               :from [:people]
               :join [:pets [:= :pets.people_id :people.id]]}))

  (testing "generation of a query with indirect joins"
    (check (parse {:select [:person/name :record/sickness]})
           => {:select [[:people.name :person/name] [:medicalrecord.sickness :record/sickness]]
               :from [:people]
               :join [:pets [:= :pets.people_id :people.id]
                      :medicalrecord [:= :pets.id :medicalrecord.pet_id]]}))

  (testing "will normalize fields on sql functions"
    (check (parse {:select [#sql/call (count :person/id)]})
           => {:select [#sql/call (count :people.id)]
               :from [:people]}))

  (testing "will normalize group, order, and having"
    (check (parse {:select [:pet/id]
                   :distinct? true
                   :group-by [:pet/name]
                   :order-by [:person/id]})
           => {:select [[:pets.id :pet/id]]
               :distinct? true
               :from [:pets]
               :join [:people [:= :pets.people_id :people.id]]
               :group-by [:pets.name]
               :order-by [:people.id]})))

(deftest generation-of-queries-with-filters
  (testing "adding joins when filtering for fields on another entity"
    (check (parse {:select [:person/name] :where [:= :pet/name "Rex"]})
           => {:select [[:people.name :person/name]]
               :from [:people]
               :join [:pets [:= :pets.people_id :people.id]]
               :where [:= :pets.name "Rex"]}))

  (testing "will not normalize subfields with specific metadata"
    (check (parse {:select [:person/name]
                   :where [:or
                           [:in :pet/name ["Rex" "Dog"]]
                           [:in :pet/name ^:walker/keep {:select [:common/pets]
                                                         :from [:common/names]}]]})
           => {:select [[:people.name :person/name]]
               :from [:people]
               :join [:pets [:= :pets.people_id :people.id]]
               :where [:or
                       [:in :pets.name ["Rex" "Dog"]]
                       [:in :pets.name {:select [:common/pets]
                                        :from [:common/names]}]]})))

(deftest non-entity-fields
  (testing "will be ignored on the select clause"
    (check (parse {:select [["wow" :some-str] :person/name :pet/name]})
           => {:select [["wow" :some-str] [:people.name :person/name] [:pets.name :pet/name]]
               :from [:people]
               :join [:pets [:= :pets.people_id :people.id]]})))

(def complex-parse (walker/parser-for
                    {:entities {:person :people
                                :child [:people :children]
                                :toy :toys
                                :preferred [:toys :preferred]
                                :house :houses}
                     :joins {[:house :person] [:= :people.house_id :houses.id]
                             [:person :child] [:= :children.parent_id :people.id]
                             [:child :toy] [:= :children.id :toys.child_id]
                             [:child :preferred] [:= :children.id :preferred.child_id]}}))

(deftest complex-queries
  (check (complex-parse {:select [:house/id :child/name]
                         :where [:and
                                 [:= :toy/name "Car"]
                                 [:= :preferred/name "Doll"]]})
         => {:select [[:houses.id :house/id] [:children.name :child/name]]
             :from [:houses]
             :join [:people [:= :people.house_id :houses.id]
                    [:people :children] [:= :children.parent_id :people.id]
                    :toys [:= :children.id :toys.child_id]
                    [:toys :preferred] [:= :children.id :preferred.child_id]]
             :where [:and
                     [:= :toys.name "Car"]
                     [:= :preferred.name "Doll"]]}))
