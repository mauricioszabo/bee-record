(ns bee-record.walker-test
  (:require [midje.sweet :refer :all]
            [bee-record.walker :as walker]))

(def mapping {:entities {:person {:from [:people]}
                         :pet {:from [:pets]}
                         :record {:from [:medicalrecord]}}
              :joins {[:person :pet] [:= :pets.people_id :people.id]
                      [:pet :record] [:= :pets.id :medicalrecord.pet_id]}})

(def parse (walker/parser-for mapping))

(facts "when generating the 'join map' (impl. detail)"
  (fact "generates a 'direct' graph"
    (#'walker/create-hierarchy {:entities {:person {:from [:people]} :pet {:from [:pets]}}
                                :joins {[:person :pet] [:= :people.id :pets.person_id]}})
    => {:person {:pet [:pets [:= :people.id :pets.person_id]]}
        :pet {:person [:people [:= :people.id :pets.person_id]]}})

  (fact "generates intermediate indexes"
    (#'walker/create-hierarchy {:entities {:person {:from [:people]}
                                           :pet {:from [:pets]}
                                           :diag {:from [:diags]}
                                           :medic {:from [:medics]}}
                                :joins {[:person :pet] [:= :people.id :pets.person_id]
                                        [:pet :diag] [:= :pets.id :diags.pet_id]
                                        [:diag :medic] [:= :diag.medic_id :medics.id]}})
    => {:person {:pet [:pets [:= :people.id :pets.person_id]]
                 :medic :diag
                 :diag :pet}
        :pet {:person [:people [:= :people.id :pets.person_id]]
              :diag [:diags [:= :pets.id :diags.pet_id]]
              :medic :diag}
        :diag {:medic [:medics [:= :diag.medic_id :medics.id]]
               :pet [:pets [:= :pets.id :diags.pet_id]]
               :person :pet}
        :medic {:diag [:diags [:= :diag.medic_id :medics.id]]
                :pet :diag
                :person :pet}}))

(fact "will generate a honey query with from"
  (parse {:select [:person/id :person/name]})
  => {:select [[:people.id :person/id] [:people.name :person/name]]
      :from [:people]})

(facts "when generating a query with a direct join"
  (parse {:select [:person/name :pet/color :pet/race]})
  => {:select [[:people.name :person/name] [:pets.color :pet/color] [:pets.race :pet/race]]
      :from [:people]
      :join [:pets [:= :pets.people_id :people.id]]})

(facts "when generating a query with indirect joins"
  (parse {:select [:person/name :record/sickness]})
  => {:select [[:people.name :person/name] [:medicalrecord.sickness :record/sickness]]
      :from [:people]
      :join [:pets [:= :pets.people_id :people.id]
             :medicalrecord [:= :pets.id :medicalrecord.pet_id]]})

; (require '[honeysql.core :as honey])
; ; ;
; (honey/format {:select [[:people.name :person/name] [:medicalrecord.sickness :record/sickness]]
;                :from [:people]
;                :join [[:pets] [:= :pets.people_id :people.id]
;                       :medicalrecord [:= :pets.id :medicalrecord.pet_id]]})
