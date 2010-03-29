(ns am.ik.clj-gae-ds.core-test
  (:use [am.ik.clj-gae-ds core test-utils] :reload-all)
  (:use [clojure.test]))

(set! *warn-on-reflection* true)

(import-deps)

(defdstest test-get-service
  (is (= (get-service) (get-service))))

(defdstest test-create-key
  (let [parent (create-key "parent" 100)]
  (is (instance? Key (create-key "foo" 100)))
  (is (instance? Key (create-key "foo" "bar")))
  (is (instance? Key (create-key "foo" :hoge)))
  (is (= "foo"  (.getKind (create-key "foo" 100))))
  (is (= 100  (.getId (create-key "foo" 100))))
  (is (= Long (class (.getId (create-key "foo" 100)))))
  (is (= "bar" (.getName (create-key "foo" "bar"))))
  (is (= ":hoge" (.getName (create-key "foo" :hoge))))
  (is (= 200 (.getId (create-key parent "child" 200))))
  (is (= "foo" (.getName (create-key parent "child" "foo"))))
  (is (= parent (.getParent (create-key parent "child" 200))))
  (is (= parent (.getParent (create-key parent "child" "foo"))))))

(defdstest test-create-entity
  (let [parent (create-key "parent" 100)]
    (is (instance? Entity (create-entity "foo")))
    (is (instance? Entity (create-entity "foo" "key")))
    (is (instance? Entity (create-entity "foo" parent)))
    (is (instance? Entity (create-entity "foo" "key" parent)))
    (is (= "foo" (.getKind (create-entity "foo"))))
    (is (= "key" (.getName (.getKey (create-entity "foo" "key")))))
    (is (= parent (.getParent (create-entity "foo" parent))))
    (is (= parent (.getParent (.getKey (create-entity "foo" parent)))))
    (is (= parent (.getParent (create-entity "foo" "keyname" parent))))
    (is (= parent (.getParent (.getKey (create-entity "foo" "keyname" parent)))))
    (is (= "keyname" (.getName (.getKey (create-entity "foo" "keyname" parent)))))))

(defdstest test-map-entity
  (let [date (java.util.Date.)]
    (is (instance? Entity (map-entity "article" :title "hoge" :date date)))
    (is (instance? Entity (map-entity "article" :title "hoge" :date date)))
    (is (= "hoge" (get-prop (map-entity "article" :title "hoge" :date date) "title")))
    (is (= date (get-prop (map-entity "article" :title "hoge" :date date) "date")))
    (is (= date (get-prop (map-entity "article" :title "hoge" :date date) "date")))
    (let [parent-key (create-key "article" 10)]
      (is (= parent-key (.getParent (map-entity "category" :name "foo" :parent parent-key))))
      (is (= "bar" (.getName (.getKey (map-entity "category" :name "foo" :keyname "bar")))))
      (is (= parent-key (.getParent (map-entity "category" :name "foo" :keyname "bar" :parent parent-key))))
      (is (= "bar" (.getName (.getKey (map-entity "category" :name "foo" :keyname "bar" :parent parent-key)))))
      )))

(defdstest test-entity-map
  (let [date (java.util.Date.) 
        e (map-entity "article" :name "hoge" :date date)]
    (= (is "hoge" ((entity-map e) "name")))
    (= (is date ((entity-map e) "date")))
    (= (nil? ((entity-map e) :parent)))
    (= (nil? ((entity-map e) :keyname)))
  (let [key (create-key "article" 100)
        e (map-entity "category" :name "foo" :parent key :keyname "kn")]
    (= (is "foo" ((entity-map e) "name")))
    (= (is key ((entity-map e) :parent)))
    (= (is "kn" ((entity-map e) :keyname))))))

(defdstest test-query
  (let [ancestor (create-key "bar" 100)]
    (= (instance? Query (query "foo")))
    (= (instance? Query (query ancestor)))
    (= (instance? Query (query "foo" ancestor)))
    (= (is ancestor (.getAncestor (query ancestor))))
    (= (is ancestor (.getAncestor (query "foo" ancestor))))))

(defdstest test-prepare 
  (= (instance? PreparedQuery (prepare (query "foo")))))

(defdstest test-ds-put
  (let [p1 (map-entity "person" :name "Bob" :age 20)
        p2 (map-entity "person" :name "John" :age 22)]
    (try 
     (is (instance? Key (ds-put p1)))
     (is (instance? Key (ds-put p2)))
     (is (= 2 (count (ds-put [p1 p2]))))
     (is (= (ds-put [p1 p2]) (ds-put [p1 p2])))
     (is (every? #(instance? Key %) (ds-put [p1 p2])))
     (finally 
      (ds-delete [(.getKey p1) (.getKey p1)])))))
      
(defdstest test-ds-get
   (let [p1 (map-entity "person" :name "Bob" :age 20)
         p2 (map-entity "person" :name "John" :age 22)]
     (try 
      (ds-put [p1 p2])
      (is (= p1 (ds-get (create-key "person" (long 1)))))
      (is (= p2 (ds-get (create-key "person" (long 2)))))
      (finally 
       (ds-delete [(.getKey p1) (.getKey p2)]) ; to escape warn on reflection
       ))))

(defdstest test-ds-delete
   (let [p1 (map-entity "person" :name "Bob" :age 20)
         p2 (map-entity "person" :name "John" :age 22)]
     (try 
      (let [ks (ds-put [p1 p2])]
        (is (= 2 (count (ds-get ks))))
        (ds-delete ks)
        (is (= 0 (count (ds-get ks)))))
      (let [k1 (ds-put p1)]
        (is (not (nil?  (ds-get k1))))
        (ds-delete k1)
        (is (nil? (ds-get k1))))
      (let [k2 (ds-put p2)]
        (is (not (nil? (ds-get k2))))
        (ds-delete k2)
        (is (nil? (ds-get k2))))
      (finally 
       (ds-delete [(.getKey p1) (.getKey p2)])))))

(defdstest test-allocate-ids 
  (let [ids (allocate-ids "person" 20)]
    (is (= 1 (.getId (.getStart ids))))
    (is (= 20 (.getId (.getEnd ids)))))
  (let [ids (allocate-ids "person" 20)]
    (is (= 21 (.getId (.getStart ids))))
    (is (= 40 (.getId (.getEnd ids))))))

(defdstest test-query-seq
  (let [p1 (map-entity "person" :name "Bob" :age 20)
        p2 (map-entity "person" :name "John" :age 22)
        p3 (map-entity "person" :name "Smith" :age 25)
        p4 (map-entity "person" :name "Ken" :age 21)]
    (try 
     (ds-put [p1 p2 p3 p4])
     (is (= 4 (count (query-seq (query "person")))))
     (finally 
      (ds-delete [(.getKey p1) (.getKey p2)])))))

;; not optimized in using with-transcation ...
(set! *warn-on-reflection* false)

(defdstest test-with-transaction1
  (is (empty? (query-seq (query "person"))))
  (with-transcation 
    (ds-put (map-entity "person" :name "John")))
  (is (= 1 (count (query-seq (query "person"))))))

(defdstest test-with-transaction2
  (is (empty? (query-seq (query "person"))))
  (try 
   (with-transcation 
     (ds-put (map-entity "person" :name "John"))
     (/ 1 0) ; fail to commit
     )
   (catch ArithmeticException e))
  (is (empty? (query-seq (query "person")))))

(defdstest test-with-transaction3
  (is (empty? (query-seq (query "person"))))  
  (ds-put (map-entity "person" :name "John"))
  (with-transcation 
    (ds-delete (create-key "person" 1)))
  (is (empty? (query-seq (query "person")))))

(defdstest test-with-transaction4
  (is (empty? (query-seq (query "person"))))  
  (ds-put (map-entity "person" :name "John"))
  (try
   (with-transcation 
     (ds-delete (create-key "person" 1))
     (/ 1 0) ; fail to commit
     )
   (catch ArithmeticException e))
  (is (= 1 (count (query-seq (query "person"))))))

;; @see https://sites.google.com/a/topgate.co.jp/systemsolution/Home/googleappengine/datastore-lowlevelapi#TOC-Transaction
(defdstest test-with-transaction5
  (is (empty? (query-seq (query "parent"))))
  (is (empty? (query-seq (query "child"))))
  (let [p (map-entity "parent" :name "Bob" :age 40)]
    (with-transcation
      (let [parent-key (ds-put p)
            child1 (map-entity "child" :name "John" :age 5 :parent parent-key)
            child2 (map-entity "child" :name "Kris" :age 3 :parent parent-key)]
        (ds-put [child1 child2])))
    (is (= 1 (count (query-seq (query "parent")))))
    (is (= 2 (count (query-seq (query "child")))))
    (is (= 2 (count (query-seq (query "child" (.getKey p))))))))

(defdstest test-with-transaction6
  (is (empty? (query-seq (query "parent"))))
  (is (empty? (query-seq (query "child"))))
  (let [p (map-entity "parent" :name "Bob" :age 40)]
    (try
     (with-transcation
       (let [parent-key (ds-put p)
             child1 (map-entity "child" :name "John" :age 5 :parent parent-key)
             child2 (map-entity "child" :name "Kris" :age 3 :parent parent-key)]
         (ds-put child1)
         (/ 1 0) ; exception occured!
         (ds-put child2)))
     (catch ArithmeticException e))
    (is (empty? (query-seq (query "parent"))))
    (is (empty? (query-seq (query "child"))))))

;; (run-tests)