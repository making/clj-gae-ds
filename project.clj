(defproject am.ik/clj-gae-ds "0.3.0"
  :description "a datastore library on Google App Engine for Clojure"
  :repositories {"maven.seasar.org" "http://maven.seasar.org/maven2",}
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [com.google.appengine/appengine-api-1.0-sdk "1.5.1"]]
  :dev-dependencies [[am.ik/clj-gae-testing "0.2.0"]
                     ;; [leiningen/lein-swank "1.2.0-SNAPSHOT"]
                     [lein-clojars "0.5.0-SNAPSHOT"]]
  :namespaces [am.ik.clj-gae-ds.core])
