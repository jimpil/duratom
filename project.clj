(defproject duratom "0.1.0-SNAPSHOT"
  :description "A durable atom type for Clojure"
  :url "https://github.com/jimpil/duratom"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.6.1"]]
  :profiles {:dev {:dependencies [[org.postgresql/postgresql "9.4.1208.jre7"]]}})
