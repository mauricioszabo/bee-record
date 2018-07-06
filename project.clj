(defproject bee-record "0.0.5-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [honeysql "0.9.2"]
                 [org.clojure/java.jdbc "0.7.7"]]
  :profiles {:dev {:dependencies [[midje "1.9.1"]
                                  [org.hsqldb/hsqldb "2.4.0"]]}})
