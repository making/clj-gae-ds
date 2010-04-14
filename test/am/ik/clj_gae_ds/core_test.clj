(ns am.ik.clj-gae-ds.core-test
  (:use [am.ik.clj-gae-ds core] :reload-all)
  (:use [am.ik.clj-gae-testing test-utils])
  (:use [clojure.test]))

(set! *warn-on-reflection* true)

(import-deps)

(defdstest test-get-ds-service
  (is (= (get-ds-service) (get-ds-service))))

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
    (is (instance? java.util.List (get-prop (map-entity "artile" :tags ["foo" "bar"]) "tags")))
    (let [parent-key (create-key "article" 10)]
      (is (= parent-key (.getParent (map-entity "category" :name "foo" :parent parent-key))))
      (is (= "bar" (.getName (.getKey (map-entity "category" :name "foo" :keyname "bar")))))
      (is (= parent-key (.getParent (map-entity "category" :name "foo" :keyname "bar" :parent parent-key))))
      (is (= "bar" (.getName (.getKey (map-entity "category" :name "foo" :keyname "bar" :parent parent-key)))))
      )))

(defdstest test-entity-map
  (let [date (java.util.Date.) 
        e (map-entity "article" :name "hoge" :date date)]
    (is (= "hoge" ((entity-map e) "name")))
    (is (= date ((entity-map e) "date")))
    (is (nil? ((entity-map e) :parent)))
    (is (nil? ((entity-map e) :keyname)))
  (let [key (create-key "article" 100)
        e (map-entity "category" :name "foo" :parent key :keyname "kn")]
    (is (= "foo" ((entity-map e) "name")))
    (is (= key ((entity-map e) :parent)))
    (is (= "kn" ((entity-map e) :keyname))))))

(defdstest test-set-get-prop
  (let [e (map-entity "article" :name "hoge")]
    (set-prop e "foo" "xxx")
    (sep e "bar" "yyy")
    (set-prop e :aaa "zzz")
    (sep e :bbb "www")
    (is (= "hoge" (gep e "name")))
    (is (= "hoge" (gep e :name)))
    (is (= "xxx" (get-prop e "foo")))
    (is (= "xxx" (gep e "foo")))
    (is (= "yyy" (get-prop e "bar")))
    (is (= "yyy" (gep e "bar")))
    (is (= "xxx" (get-prop e :foo)))
    (is (= "xxx" (gep e :foo)))
    (is (= "yyy" (get-prop e :bar)))
    (is (= "yyy" (gep e :bar)))
    (is (= "zzz" (get-prop e "aaa")))
    (is (= "zzz" (gep e "aaa")))
    (is (= "www" (get-prop e "bbb")))
    (is (= "www" (gep e "bbb")))
    (is (= "zzz" (get-prop e :aaa)))
    (is (= "zzz" (gep e :aaa)))
    (is (= "www" (get-prop e :bbb)))
    (is (= "www" (gep e :bbb)))))

(defdstest test-query
  (let [ancestor (create-key "bar" 100)]
    (is (instance? Query (query "foo")))
    (is (instance? Query (query ancestor)))
    (is (instance? Query (query "foo" ancestor)))
    (is (= ancestor (.getAncestor (query ancestor))))
    (is (= ancestor (.getAncestor (query "foo" ancestor))))
    (is (instance? Query (q "foo")))
    (is (instance? Query (q ancestor)))
    (is (instance? Query (q "foo" ancestor)))
    (is (= ancestor (.getAncestor (q ancestor))))
    (is (= ancestor (.getAncestor (q "foo" ancestor))))))

(defdstest test-prepare 
  (= (instance? PreparedQuery (prepare (query "foo")))))

(defdstest test-ds-put
  (let [p1 (map-entity "person" :name "Bob" :age 20)
        p2 (map-entity "person" :name "John" :age 22)]
    (is (instance? Key (ds-put p1)))
    (is (instance? Key (ds-put p2)))
    (is (= 2 (count (ds-put [p1 p2]))))
    (is (= (ds-put [p1 p2]) (ds-put [p1 p2])))
    (is (every? #(instance? Key %) (ds-put [p1 p2])))))
    
      
(defdstest test-ds-get
  (let [p1 (map-entity "person" :name "Bob" :age 20)
        p2 (map-entity "person" :name "John" :age 22)]
    (ds-put [p1 p2])
    (is (= p1 (ds-get (create-key "person" 1))))
    (is (= p2 (ds-get (create-key "person" 2))))))

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

(defdstest test-get-key
  (let [e (map-entity "e" :name "foo")
        k (ds-put e)]
    (is (= k (get-key e)))))

(defdstest test-get-parent
  (let [pk (create-key "p" 100)
        e (map-entity "e" :name "foo" :parent pk)]
    (ds-put e)
    (is (= pk (get-parent e)))))

(defdstest test-get-id
  (let [k1 (create-key "p" 100)
        k2 (create-key "p" "xxx")]
    (is (= 100 (get-id k1)))
    (is (instance? Long (get-id k1)))
    (is (= 0 (get-id k2)))))

(defdstest test-get-name
  (let [k1 (create-key "p" 100)
        k2 (create-key "p" "xxx")]
    (is (nil? (get-name k1)))
    (is (= "xxx" (get-name k2)))))

(defdstest test-get-kind
  (let [k1 (create-key "p" 100)
        k2 (create-key "p" "xxx")]
    (is (= "p" (get-kind k1)))
    (is (= "p" (get-kind k2)))))

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
    (ds-put [p1 p2 p3 p4])
    (is (= 4 (count (query-seq (query "person")))))))


(defdstest test-count-entities
  (let [p1 (map-entity "person" :name "Bob" :age 20)
        p2 (map-entity "person" :name "John" :age 22)
        p3 (map-entity "person" :name "Smith" :age 25)
        p4 (map-entity "person" :name "Ken" :age 21)]
    (is (= 0 (count-entities (query "person"))))
    (ds-put [p1 p2 p3 p4])
    (is (= 4 (count-entities (query "person"))))))

(defdstest test-count-entities
  (let [p1 (map-entity "person" :name "Bob" :age 20)
        p2 (map-entity "person" :name "John" :age 22)
        p3 (map-entity "person" :name "Smith" :age 25)
        p4 (map-entity "person" :name "Ken" :age 21)]
    (is (= 0 (count-entities (prepare (query "person")))))
    (ds-put [p1 p2 p3 p4])
    (is (= 4 (count-entities (prepare (query "person")))))))

(defdstest test-add-sort
  (let [n1 (map-entity "num" :val 1)
        n2 (map-entity "num" :val 2)
        n3 (map-entity "num" :val 3)]
    (ds-put [n2 n1 n3]) ; insert 2, 1, 3
    (is (= (list n1 n2 n3) (query-seq (add-sort (query "num") "val" :asc))))
    (is (= (list n3 n2 n1) (query-seq (add-sort (query "num") "val" :desc))))
    (is (= (list n1 n2 n3) (query-seq (add-sort (query "num") "val" Query$SortDirection/ASCENDING))))
    (is (= (list n3 n2 n1) (query-seq (add-sort (query "num") "val" Query$SortDirection/DESCENDING))))
    (is (= (list n1 n2 n3) (query-seq (srt (q "num") "val" :asc))))
    (is (= (list n3 n2 n1) (query-seq (srt (q "num") "val" :desc))))
    (is (= (list n1 n2 n3) (query-seq (-> (q "num") (srt "val" :asc)))))
    (is (= (list n1 n2 n3) (query-seq (add-sort (query "num") :val :asc))))
    (is (= (list n3 n2 n1) (query-seq (add-sort (query "num") :val :desc))))
    (is (= (list n1 n2 n3) (query-seq (srt (q "num") :val :asc))))
    (is (= (list n3 n2 n1) (query-seq (srt (q "num") :val :desc))))
    (is (= (list n1 n2 n3) (query-seq (-> (q "num") (srt :val :asc)))))))

(defdstest test-add-filter
  (let [n1 (map-entity "num" :val 1)
        n2 (map-entity "num" :val 2)
        n3 (map-entity "num" :val 3)]
    (ds-put [n1 n2 n3])
    (is (= (list n1) (query-seq (add-filter (query "num") "val" = 1))))
    (is (= (list n2 n3) (query-seq (add-filter (query "num") "val" not= 1))))
    (is (= (list n3) (query-seq (add-filter (query "num") "val" > 2))))
    (is (= (list n2 n3) (query-seq (add-filter (query "num") "val" >= 2))))
    (is (= (list n1) (query-seq (add-filter (query "num") "val" < 2))))
    (is (= (list n1 n2) (query-seq (add-filter (query "num") "val" <= 2))))
    (is (= (list n1) (query-seq (add-filter (query "num") "val" :eq 1))))
    (is (= (list n2 n3) (query-seq (add-filter (query "num") "val" :neq 1))))
    (is (= (list n3) (query-seq (add-filter (query "num") "val" :gt 2))))
    (is (= (list n2 n3) (query-seq (add-filter (query "num") "val" :gte 2))))
    (is (= (list n1) (query-seq (add-filter (query "num") "val" :lt 2))))
    (is (= (list n1 n2) (query-seq (add-filter (query "num") "val" :lte 2))))
    (is (= (list n1 n2) (query-seq (add-filter (query "num") "val" :in [1 2]))))
    ;;
    (is (= (list n1) (query-seq (flt (q "num") "val" = 1))))
    (is (= (list n2 n3) (query-seq (flt (q "num") "val" not= 1))))
    (is (= (list n3) (query-seq (flt (q "num") "val" > 2))))
    (is (= (list n2 n3) (query-seq (flt (q "num") "val" >= 2))))
    (is (= (list n1) (query-seq (flt (q "num") "val" < 2))))
    (is (= (list n1 n2) (query-seq (flt (q "num") "val" <= 2))))
    (is (= (list n1) (query-seq (flt (q "num") "val" :eq 1))))
    (is (= (list n2 n3) (query-seq (flt (q "num") "val" :neq 1))))
    (is (= (list n3) (query-seq (flt (q "num") "val" :gt 2))))
    (is (= (list n2 n3) (query-seq (flt (q "num") "val" :gte 2))))
    (is (= (list n1) (query-seq (flt (q "num") "val" :lt 2))))
    (is (= (list n1 n2) (query-seq (flt (q "num") "val" :lte 2))))
    (is (= (list n1 n2) (query-seq (flt (q "num") "val" :in [1 2]))))
    ;;
    (is (= (list n1) (query-seq (-> (q "num") (flt "val" = 1)))))
    (is (= (list n1) (query-seq (add-filter (query "num") :val = 1))))
    (is (= (list n2 n3) (query-seq (add-filter (query "num") :val not= 1))))
    (is (= (list n3) (query-seq (add-filter (query "num") :val > 2))))
    (is (= (list n2 n3) (query-seq (add-filter (query "num") :val >= 2))))
    (is (= (list n1) (query-seq (add-filter (query "num") :val < 2))))
    (is (= (list n1 n2) (query-seq (add-filter (query "num") :val <= 2))))
    (is (= (list n1) (query-seq (add-filter (query "num") :val :eq 1))))
    (is (= (list n2 n3) (query-seq (add-filter (query "num") :val :neq 1))))
    (is (= (list n3) (query-seq (add-filter (query "num") :val :gt 2))))
    (is (= (list n2 n3) (query-seq (add-filter (query "num") :val :gte 2))))
    (is (= (list n1) (query-seq (add-filter (query "num") :val :lt 2))))
    (is (= (list n1 n2) (query-seq (add-filter (query "num") :val :lte 2))))
    (is (= (list n1 n2) (query-seq (add-filter (query "num") :val :in [1 2]))))
    ;;
    (is (= (list n1) (query-seq (flt (q "num") :val = 1))))
    (is (= (list n2 n3) (query-seq (flt (q "num") :val not= 1))))
    (is (= (list n3) (query-seq (flt (q "num") :val > 2))))
    (is (= (list n2 n3) (query-seq (flt (q "num") :val >= 2))))
    (is (= (list n1) (query-seq (flt (q "num") :val < 2))))
    (is (= (list n1 n2) (query-seq (flt (q "num") :val <= 2))))
    (is (= (list n1) (query-seq (flt (q "num") :val :eq 1))))
    (is (= (list n2 n3) (query-seq (flt (q "num") :val :neq 1))))
    (is (= (list n3) (query-seq (flt (q "num") :val :gt 2))))
    (is (= (list n2 n3) (query-seq (flt (q "num") :val :gte 2))))
    (is (= (list n1) (query-seq (flt (q "num") :val :lt 2))))
    (is (= (list n1 n2) (query-seq (flt (q "num") :val :lte 2))))
    (is (= (list n1 n2) (query-seq (flt (q "num") :val :in [1 2]))))
    (is (= (list n1) (query-seq (-> (q "num") (flt :val = 1)))))))

;; not optimized in using with-transaction ...
(set! *warn-on-reflection* false)

(defdstest test-with-transaction1
  (is (empty? (query-seq (query "person"))))
  (with-transaction 
    (ds-put (map-entity "person" :name "John")))
  (is (= 1 (count (query-seq (query "person"))))))

(defdstest test-with-transaction2
  (is (empty? (query-seq (query "person"))))
  (try 
   (with-transaction 
     (ds-put (map-entity "person" :name "John"))
     (/ 1 0) ; fail to commit
     )
   (catch ArithmeticException e))
  (is (empty? (query-seq (query "person")))))

(defdstest test-with-transaction3
  (is (empty? (query-seq (query "person"))))  
  (ds-put (map-entity "person" :name "John"))
  (with-transaction 
    (ds-delete (create-key "person" 1)))
  (is (empty? (query-seq (query "person")))))

(defdstest test-with-transaction4
  (is (empty? (query-seq (query "person"))))  
  (ds-put (map-entity "person" :name "John"))
  (try
   (with-transaction 
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
    (with-transaction
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
     (with-transaction
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