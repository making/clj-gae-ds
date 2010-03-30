(defproject am.ik/clj-gae-ds "0.1.0"
  :description "a datastore library on Google App Engine for Clojure"
  :repositories {"gaejtools" "http://gaejtools.sourceforge.jp/maven/repository",
                 "maven.seasar.org" "http://maven.seasar.org/maven2"}
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0"]
                 [com.google.appengine/appengine-api-1.0-sdk "1.3.1"]]
  :dev-dependencies [[com.google.appengine/appengine-testing "1.3.1"]
                     [com.google.appengine/appengine-api-labs "1.3.1"]
                     [com.google.appengine/appengine-api-1.0-stubs "1.3.1"]                     
                     ;; [com.google.appengine/appengine-api-1.0-runtime "1.3.1"]
                     ;; [com.google.appengine/appengine-tools-sdk "1.3.1"]
                     ;; [com.google.appengine/appengine-api-1.0-runtime-shared "1.3.1"]
                     ;; [leiningen/lein-swank "1.1.0"]
                     [lein-clojars "0.5.0-SNAPSHOT"]
                     ] 
  :namespaces [am.ik.clj-gae-ds.core])
