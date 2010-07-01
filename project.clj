(defproject am.ik/clj-gae-ds "0.3.0-SNAPSHOT"
  :description "a datastore library on Google App Engine for Clojure"
  :repositories {"maven.seasar.org" "http://maven.seasar.org/maven2",}
  :dependencies [[org.clojure/clojure "1.2.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.2.0-SNAPSHOT"]
                 [com.google.appengine/appengine-api-1.0-sdk "1.3.4"]]
  :dev-dependencies [[am.ik/clj-gae-testing "0.2.0-SNAPSHOT"]
                     [leiningen/lein-swank "1.1.0"]
                     [lein-clojars "0.5.0-SNAPSHOT"]]
  :namespaces [am.ik.clj-gae-ds.core])
