# BeeRecord
_Where "ActiveRecord" encounters "HoneySQL"..._

[![Clojars Project](https://img.shields.io/clojars/v/bee-record.svg)](https://clojars.org/bee-record)
[![Build Status](https://travis-ci.org/mauricioszabo/bee-record.svg?branch=master)](https://travis-ci.org/mauricioszabo/bee-record)

BeeRecord is an wrapper to HoneySQL. It maps Clojure maps to something resembling "records" from the ActiveRecord patterns, and creates some functions to allow us to manipulate these records in a simple way.

Heavily inspired by Ruby's ActiveRecord (and Sequel), it allows us to generate queries, to query things, and to update records. Different from ActiveRecord and Sequel, it has a separate API to query things and to update things.

## Introduction

You first must create a mapping. It is just a Clojure map, but we do have a helper function that helps us to generate the correct map and transform some fields to what we expect too:

```clojure
(require '[bee-record.sql :as sql])
(def user
  (sql/model {:pk :id
              :table :users
              :fields [:id :name :age]}))
```

Then, we can query things:

```clojure
(-> user
    (sql/select [:name])
    (sql/distinct)
    (sql/where [:= :id 20])
    (sql/query db))
```

## Why?
SQL is **hard**. Ok, let's rephrase that: SQL is not _that hard_, but it becomes harder and harder the more you need conditional SQL. Let's begin by the simpler example possible: you want to filter users on your system. To filter it, you need to concatenate SQL fragments depending on what the fields the user wants to filter, or add joins, or transform a join from inner to left... then things become complicated, messy, and when you see you're just fighting with string concatenation instead of working the logic in your app.

Clojure already have a wonderful library that creates a single SQL query for us: it's HoneySQL. It's a wonderful library that expects a clojure map and creates a query pair (string + fields) that we can send to `clojure.java.jdbc/query`. It's possible to work with HoneySQL alone, and it's already a million times better than working without it:

```clojure
(require '[honeysql.core :as honey]
         '[clojure.java.jdbc :as jdbc])

(def people {:from [:people] :select [:id :name]})
(jdbc/query db (honey/format people :quoting :ansi))
=> [{:id 1, :name "Foo"}
    {:id 2, :name "Bar"}
    {:id 3, :name "Baz"}]
```

Then, if you want to filter something, you just `assoc` on the `people` var a `:where` clause, and everything works. But this covers _one query only_. If we want to use multiple queries (preloads, for instance), we're out of luck. Also, we still need to join by hand, and unless you make an opinated function that derives your foreign keys and primary keys, then do the joins, you'll have to type more.

These things for me are a **big deal**: I want something like Sequel for Ruby: to make simple queries... well, simple... and to make complex queries possible.

So, to represent the same mapping above, we would use the following code in BeeRecord:

```clojure
(require '[bee-record.sql :as sql])

(def people (sql/model {:table :people :fields [:id :name] :pk :id}))
(sql/query people db)
=> [{:people/id 1, :people/name "Foo"}
    {:people/id 2, :people/name "Bar"}
    {:people/id 3, :people/name "Baz"}]
```

With this approach, joins become easier:

```clojure
(require '[bee-record.sql :as sql])

(def accounts (sql/model {:table :accounts
                          :fields [:id :account :user-id]
                          :pk :id}))
(def people (sql/model {:table :people
                        :fields [:id :name]
                        :pk :id
                        :associations {:accounts {:model accounts
                                                  :on {:people/id :accounts/user-id}}}}))
(-> people
    (sql/join :inner :accounts)
    (sql/query db))
=> [{:people/id 1, :people/name "Foo"}
    {:people/id 2, :people/name "Bar"}]

;; Or, you can include fields from the join:
(-> people
    (sql/join :inner {:accounts {:opts {:include-fields true}}})
    (sql/query db))
=> [{:people/id 1,
     :people/name "Foo",
     :accounts/id 1,
     :accounts/account "twitter",
     :accounts/user-id 1}
    {:people/id 1,
     :people/name "Foo",
     :accounts/id 2,
     :accounts/account "fb",
     :accounts/user-id 1}]
```

## Select

## Conditions (WHERE)

## Joins

```clojure
```

## Preload

## What is still missing
* [ ] There is no error treatment. When something go wrong, we'll probably get a Null Pointer Exception. It would be good to have an error, like "your association doesn't exist", or something
* [ ] There is no "UPDATE" or "INSERT" APIs yet. It would be good to implement a Changeset pattern like ECTO
* [x] Scopes
* [x] Preloading of Scopes

## License

Copyright © 2018 Maurício Szabo

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
