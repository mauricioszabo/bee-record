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
    (is (match? {:select [[:people.name :person/name] [:medicalrecord.sickness :record/sickness]]
                 :from [:people]
                 :join [:pets [:= :pets.people_id :people.id]
                        :medicalrecord [:= :pets.id :medicalrecord.pet_id]]}
                (parse {:select [:person/name :record/sickness]})))))

(deftest generation-of-queries-with-filters
  (testing "adding joins when filtering for fields on another entity"
    (is (match? {:select [[:people.name :person/name]]
                 :from [:people]
                 :join [:pets [:= :pets.people_id :people.id]]
                 :where [:= :pets.name "Rex"]}
                (parse {:select [:person/name] :where [:= :pet/name "Rex"]}))))

  (testing "will not normalize fields for 'in' queries"
    (is (match? {:select [[:people.name :person/name]]
                 :from [:people]
                 :join [:pets [:= :pets.people_id :people.id]]
                 :where [:or
                         [:in :pets.name ["Rex" "Dog"]]
                         [:in :pets.name {:select [:common/pets]
                                          :from [:common/names]}]]}
                (parse {:select [:person/name]
                        :where [:or
                                [:in :pet/name ["Rex" "Dog"]]
                                [:in :pet/name {:select [:common/pets]
                                                :from [:common/names]}]]})))))

(deftest non-entity-fields
  (testing "will be ignored on the select clause"
    (is (match? {:select [["wow" :some-str] [:people.name :person/name] [:pets.name :pet/name]]
                 :from [:people]
                 :join [:pets [:= :pets.people_id :people.id]]}
                (parse {:select [["wow" :some-str] :person/name :pet/name]})))))

(def complex-parse (walker/parser-for
                    {:entities {:person {:from [:people]}
                                :child {:from [[:people :children]]}
                                :toy {:from [:toys]}
                                :preferred {:from [[:toys :preferred]]}
                                :house {:from [:houses]}}
                     :joins {[:house :person] [:= :people.house_id :houses.id]
                             [:person :child] [:= :children.parent_id :people.id]
                             [:child :toy] [:= :children.id :toys.child_id]
                             [:child :preferred] [:= :children.id :preferred.child_id]}}))

(deftest complex-queries
  (is (match? {:select [[:houses.id :house/id] [:children.name :child/name]]
               :from [:houses]
               :join [:people [:= :people.house_id :houses.id]
                      [:people :children] [:= :children.parent_id :people.id]
                      :toys [:= :children.id :toys.child_id]
                      [:toys :preferred] [:= :children.id :preferred.child_id]]
               :where [:and
                       [:= :toys.name "Car"]
                       [:= :preferred.name "Doll"]]}
              (complex-parse {:select [:house/id :child/name]
                              :where [:and
                                      [:= :toy/name "Car"]
                                      [:= :preferred/name "Doll"]]}))))
; (ns bee-record.walker-test
;   (:require [midje.sweet :refer :all]
;             [bee-record.walker :as walker]))
;
; (def mapping {:entities {:person {:from [:people]}
;                          :pet {:from [:pets]}
;                          :record {:from [:medicalrecord]}}
;               :joins {[:person :pet] [:= :pets.people_id :people.id]
;                       [:pet :record] [:= :pets.id :medicalrecord.pet_id]}})
;
; (def parse (walker/parser-for mapping))
;
; (facts "about mapping of identities to DB fields"
;   (fact "will generate a honey query with from"
;     (parse {:select [:person/id :person/name]})
;     => {:select [[:people.id :person/id] [:people.name :person/name]]
;         :from [:people]})
;
;   (fact "will generate a honey query with alias on select"
;     (parse {:select [[:person/id :pet/name]]})
;     => {:select [[:people.id :pet/name]]
;         :from [:people]})
;
;   (fact "will normalize fields on sql functions"
;     (parse {:select [#sql/call (count :person/id)]})
;     => {:select [#sql/call (count :people.id)]
;         :from [:people]}
;
;     (parse {:select [[#sql/call (count :id) :foo]]})
;     => {:select [[#sql/call (count :id) :foo]]})
;
;   (fact "will normalize IF (complex select)"
;     (parse {:select [#sql/call (if #sql/call [:> (count :person/id) 1]
;                                  "foo"
;                                  "bar")]})
;     => {:select [#sql/call (if #sql/call [:> (count :people.id) 1]
;                              "foo"
;                              "bar")]
;         :from [:people]}))
;
; (facts "will generate a query with a direct join"
;   (parse {:select [:person/name :pet/color :pet/race]})
;   => {:select [[:people.name :person/name] [:pets.color :pet/color] [:pets.race :pet/race]]
;       :from [:people]
;       :join [:pets [:= :pets.people_id :people.id]]})
;
; (facts "will generate a query with indirect joins"
;   (parse {:select [:person/name :record/sickness]})
;   => {:select [[:people.name :person/name] [:medicalrecord.sickness :record/sickness]]
;       :from [:people]
;       :join [:pets [:= :pets.people_id :people.id]
;              :medicalrecord [:= :pets.id :medicalrecord.pet_id]]})
;
; (facts "when generating a query with filters"
;   (fact "add joins when filtering for fields on another entity"
;     (parse {:select [:person/name] :where [:= :pet/name "Rex"]})
;     => {:select [[:people.name :person/name]]
;         :from [:people]
;         :join [:pets [:= :pets.people_id :people.id]]
;         :where [:= :pets.name "Rex"]})
;
;   (fact "will not normalize fields for 'in' queries"
;     (parse {:select [:person/name] :where [:or
;                                            [:in :pet/name ["Rex" "Dog"]]
;                                            [:in :pet/name {:select [:common/pets]
;                                                            :from [:common/names]}]]})
;     => {:select [[:people.name :person/name]]
;         :from [:people]
;         :join [:pets [:= :pets.people_id :people.id]]
;         :where [:or
;                 [:in :pets.name ["Rex" "Dog"]]
;                 [:in :pets.name {:select [:common/pets]
;                                  :from [:common/names]}]]}))
;
; (fact "normalize group-by, order-by, and keep other attributes"
;   (parse {:select [:pet/id]
;           :distinct? true
;           :group-by [:pet/name]
;           :order-by [:person/id]})
;   => {:select [[:pets.id :pet/id]]
;       :distinct? true
;       :from [:pets]
;       :join [:people [:= :pets.people_id :people.id]]
;       :group-by [:pets.name]
;       :order-by [:people.id]})
;
; (facts "about non-entity fields"
;   (fact "will be ignored on the select clause"
;     (parse {:select [["wow" :some-str] :person/name :pet/name]})
;     => {:select [["wow" :some-str] [:people.name :person/name] [:pets.name :pet/name]]
;         :from [:people]
;         :join [:pets [:= :pets.people_id :people.id]]}))
