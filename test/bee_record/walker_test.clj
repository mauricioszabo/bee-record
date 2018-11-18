(ns bee-record.walker-test
  (:require [midje.sweet :refer :all]
            [bee-record.walker :as walker]))

(def mapping {:entities {:person {:from [:people]}
                         :pet {:from [:pets]}
                         :record {:from [:medicalrecord]}}
              :joins {[:person :pet] [:= :pets.people_id :people.id]
                      [:pet :record] [:= :pets.id :medicalrecord.pet_id]}})

(fact "will generate a honey query with from"
  (walker/parse-query mapping {:select [:person/id :person/name]})
  => {:select [[:people.id :person/id] [:people.name :person/name]]
      :from [:people]})

(facts "when generating a query with a direct join"
  (walker/parse-query mapping {:select [:person/name :pet/color :pet/race]})
  => {:select [[:people.name :person/name] [:pets.color :pet/color] [:pets.race :pet/race]]
      :from [:people]
      :join [:pets [:= :pets.people_id :people.id]]})

(facts "when generating a query with indirect joins"
  (walker/parse-query mapping {:select [:person/name :record/sickness]})
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
