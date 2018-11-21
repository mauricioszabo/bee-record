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

(facts "about mapping of identities to DB fields"
  (fact "will generate a select without from if not entity is found"
    (parse {:select ["foo"]}) => {:select ["foo"]})

  (fact "will generate a honey query with from"
    (parse {:select [:person/id :person/name]})
    => {:select [[:people.id :person/id] [:people.name :person/name]]
        :from [:people]})

  (fact "will generate a honey query with alias on select"
    (parse {:select [[:person/id :pet/name]]})
    => {:select [[:people.id :pet/name]]
        :from [:people]})

  (fact "will normalize fields on sql functions"
    (parse {:select [#sql/call (count :person/id)]})
    => {:select [#sql/call (count :people.id)]
        :from [:people]}

    (parse {:select [[#sql/call (count :id) :foo]]})
    => {:select [[#sql/call (count :id) :foo]]})

  (fact "will normalize IF (complex select)"
    (parse {:select [#sql/call (if #sql/call [:> (count :person/id) 1]
                                 "foo"
                                 "bar")]})
    => {:select [#sql/call (if #sql/call [:> (count :people.id) 1]
                             "foo"
                             "bar")]
        :from [:people]}))

(facts "will generate a query with a direct join"
  (parse {:select [:person/name :pet/color :pet/race]})
  => {:select [[:people.name :person/name] [:pets.color :pet/color] [:pets.race :pet/race]]
      :from [:people]
      :join [:pets [:= :pets.people_id :people.id]]})

(facts "will generate a query with indirect joins"
  (parse {:select [:person/name :record/sickness]})
  => {:select [[:people.name :person/name] [:medicalrecord.sickness :record/sickness]]
      :from [:people]
      :join [:pets [:= :pets.people_id :people.id]
             :medicalrecord [:= :pets.id :medicalrecord.pet_id]]})

(facts "when generating a query with filters"
  (fact "add joins when filtering for fields on another entity"
    (parse {:select [:person/name] :where [:= :pet/name "Rex"]})
    => {:select [[:people.name :person/name]]
        :from [:people]
        :join [:pets [:= :pets.people_id :people.id]]
        :where [:= :pets.name "Rex"]})

  (fact "will not normalize fields for 'in' queries"
    (parse {:select [:person/name] :where [:or
                                           [:in :pet/name ["Rex" "Dog"]]
                                           [:in :pet/name {:select [:common/pets]
                                                           :from [:common/names]}]]})
    => {:select [[:people.name :person/name]]
        :from [:people]
        :join [:pets [:= :pets.people_id :people.id]]
        :where [:or
                [:in :pets.name ["Rex" "Dog"]]
                [:in :pets.name {:select [:common/pets]
                                 :from [:common/names]}]]}))

(facts "about non-entity fields"
  (fact "will be ignored on the select clause"
    (parse {:select [["wow" :some-str] :person/name :pet/name]})
    => {:select [["wow" :some-str] [:people.name :person/name] [:pets.name :pet/name]]
        :from [:people]
        :join [:pets [:= :pets.people_id :people.id]]}))
