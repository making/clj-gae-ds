(ns am.ik.clj_gae_ds.test-utils
  (:use [clojure.test]
        [clojure.contrib.singleton])
  (:import [com.google.appengine.tools.development.testing 
            LocalDatastoreServiceTestConfig 
            LocalServiceTestHelper]))

(defn import-deps []
  (import [com.google.appengine.api.datastore 
           DatastoreServiceFactory DatastoreService 
           Entity Key KeyFactory KeyRange
           Query Query$FilterOperator PreparedQuery
           Transaction])
  nil)

(def get-helper (global-singleton 
                 #(let [config# (LocalDatastoreServiceTestConfig.)
                        helper# (LocalServiceTestHelper. (into-array [config#]))]
                    helper#)))

(defn setup [#^LocalServiceTestHelper helper]
  (.setUp helper))

(defn setup-helper []
  (setup (get-helper)))

(defn teardown [#^LocalServiceTestHelper helper]
  (.tearDown helper))

(defn teardown-helper []
  (teardown (get-helper)))

(defmacro defdstest [test-name & body]
  `(deftest ~test-name
     (let [helper# (get-helper)]
       (setup helper#)
       (try 
        ~@body
        (finally 
         (teardown helper#))))))

