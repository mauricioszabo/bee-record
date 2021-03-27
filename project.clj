(defproject bee-record "0.1.0"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [honeysql "1.0.461"]
                 [org.clojure/java.jdbc "0.7.7"]
                 [aysylu/loom "1.0.2"]]

  :profiles {:dev {:dependencies [[midje "1.9.9"]
                                  [check "0.2.0-SNAPSHOT"]
                                  [org.hsqldb/hsqldb "2.4.0"]]
                   :plugins [[lein-midje "3.2.1"]]}})
