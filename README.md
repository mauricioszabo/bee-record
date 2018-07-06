# Bee Record
_Where "ActiveRecord" encounters "HoneySQL"..._

[![Clojars Project](https://img.shields.io/clojars/v/bee-record.svg)](https://clojars.org/bee-record)

Bee Record is an wrapper to HoneySQL. It maps Clojure maps to something resembling "records" from the ActiveRecord patterns, and creates some functions to allow us to manipulate these records in a simple way.

Heavily inspired by Ruby's ActiveRecord (and Sequel), it allows us to generate queries, to query things, and to update records. Different from ActiveRecord and Sequel, it has a separate API to query things and to update things.

## Usage

You first must create a mapping. It is just a Clojure map, but as we can forget to pass some fields, there's a helper function that helps us to generate the correct map:

```clojure
(require '[bee-record.sql :as sql])
(def user
  (sql/model {:pk :id
              :table "users"
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

## What is still missing
* [ ] There is no error treatment. When something go wrong, we'll probably get a Null Pointer Exception. It would be good to have an error, like "your association doesn't exist", or something
** [x] Error treatment for invalid/unexisting associations
* [ ] There is no "UPDATE" or "INSERT" APIs yet. It would be good to implement a Changeset pattern like ECTO
* [ ] Scopes
* [ ] Preloading of Scopes

## License

Copyright © 2018 Maurício Szabo

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
