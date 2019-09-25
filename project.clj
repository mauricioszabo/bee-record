(defproject bee-record "0.1.0"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [honeysql "0.9.4"]
                 [org.clojure/java.jdbc "0.7.7"]]
  :profiles {:dev {:dependencies [[midje "1.9.9"]
                                  [nubank/matcher-combinators "1.2.1"]
                                  [org.hsqldb/hsqldb "2.4.0"]]
                   :plugins [[lein-midje "3.2.1"]]}})
