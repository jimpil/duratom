(defproject duratom "0.4.9"
  :description "A durable atom/agent type for Clojure."
  :url "https://github.com/jimpil/duratom"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1" :scope "provided"]]
  :profiles {:dev {:dependencies [[org.clojure/java.jdbc "0.6.1"]
                                  [org.postgresql/postgresql "9.4.1208.jre7"] ;; PGSQL driver
                                  [amazonica "0.3.58"]
                                  [com.taoensso/carmine "2.19.1"]
                                  [com.taoensso/nippy "2.13.0"]]}}
  :source-paths      ["src/clojure"]
  :java-source-paths ["src/java"]
  :javac-options     ["--release" "8"]
  :deploy-repositories [["releases" :clojars]] ;; lein release :patch
  :signing {:gpg-key "jimpil1985@gmail.com"}
  )
