(ns am.ik.clj-gae-ds.core
  (:use [clojure.test]
        [clojure.contrib.singleton]
        [clojure.contrib.seq-utils])
  (:import [com.google.appengine.api.datastore 
            DatastoreServiceFactory DatastoreService 
            Entity Key KeyFactory KeyRange
            Query Query$FilterOperator PreparedQuery
            Transaction]))
(def 
 #^{:arglists '([])
    :doc "get DatastoreService. This method returns singleton instance of the service."} 
 #^DatastoreService
 get-service (global-singleton #(DatastoreServiceFactory/getDatastoreService)))

;; Key
(defn #^Key create-key 
  "create key."
  ([parent kind id-or-name]
     (KeyFactory/createKey parent kind (if (integer? id-or-name) (long id-or-name) (str id-or-name))))
  ([kind id-or-name]
     (KeyFactory/createKey kind (if (integer? id-or-name) (long id-or-name) (str id-or-name)))))

;; Entity
(defn #^Entity create-entity
  "create entity."
  ([kind] (Entity. kind))
  ([kind keyname-or-parent] (Entity. kind keyname-or-parent))
  ([kind #^String keyname #^Key parent] (Entity. kind (str keyname) parent)))

(defn #^Entity map-entity
  "create Entity from map.  
   (map-entity \"person\" :name \"Bob\" :age 25)
   -> #<Entity <Entity [person(no-id-yet)]:
	    name = Bob
        age = 25>>

  To set key, use keyword, :keyname <java.lang.String> 
  , and to set parent, :parent <com.google.appengine.api.datastore.Key>.

  (map-entity \"person\" :name \"John\" :age 30 :parent (create-key \"parent\" \"xxx\"))
  #<Entity <Entity [parent(\"xxx\")/person(no-id-yet)]:
	  name = John
      age = 30>>
  So cannot include :keyname and :parent in key of the map.
  "
  [ename & inits]  
  (let [init-map (apply hash-map inits)
        keyname (:keyname init-map)
        parent (:parent init-map)
        entity-arity (filter #(not (nil? %)) [keyname parent])
        m (apply array-map (flatten (vec (dissoc init-map :keyname :parent))))
        #^Entity entity (apply create-entity ename entity-arity)]
    (doseq [e m]
      (.setProperty entity (name (first e)) (last e)))
    entity))

(defn entity-map 
  "convert entity to map"
  [#^Entity entity]
  (into {:keyname (.getName (.getKey entity)) :parent (.getParent entity)}
        (.getProperties entity)))

(defn get-prop   
  "get property"
  [#^Entity entity #^String key]
  (.getProperty entity key))

;; Query
(defn #^Query query 
  "create query."
  ([kind-or-ancestor] (Query. kind-or-ancestor))
  ([kind ancestor] (Query. kind ancestor)))

(defn prepare 
  "parepare query."
  [#^Query q]     
  (.prepare (get-service) q))

(defn query-seq   
  "return sequence made from the result of query."
  [#^DatastoreService #^Query q]
  (lazy-seq (.asIterable (prepare q))))

;; Datestore
(defn ds-put 
  "put entity to datastore"
  ([entity-or-entities]
     (.put (get-service) entity-or-entities))
  ([#^Transaction txn entity-or-entities]
     (.put (get-service) txn entity-or-entities)))

(defn ds-get 
  "get entity from datastore.
   If entity is not found, return nil.
  "
  ([key-or-keys]
     (try 
      (.get (get-service) key-or-keys)
      (catch Throwable e nil)))
  ([#^Transaction txn key-or-keys]
     (try (.get (get-service) txn key-or-keys)
          (catch Throwable e nil))))

(defn ds-delete
  "delete entity from datastore"
  ([key-or-keys]
     (.delete (get-service) (if (instance? Iterable key-or-keys) key-or-keys [key-or-keys])))
  ([#^Transaction txn key-or-keys]
     (.delete (get-service) txn (if (instance? Iterable key-or-keys) key-or-keys [key-or-keys]))))

(defn #^KeyRange allocate-ids 
 ([kind num] (.allocateIds (get-service) kind num))
 ([parent-key kind num] (.allocateIds (get-service) parent-key kind num)))

(defn allocate-id-seq
 ([kind num] (lazy-seq (allocate-ids kind num)))
 ([parent-key kind num] (lazy-seq (allocate-ids parent-key kind num))))

(defn- transactional-fn? [x]
  (or (= x 'ds-put) (= x 'ds-get ) (= x 'ds-delete)))

(defn- insert-txn [txn sexp]  
  "push front transation object before datastore operation"
  (if (and (coll? sexp) (not-empty sexp))
    (cond (= (count sexp) 1) (if (vector? sexp) [(insert-txn txn (first sexp))] 
                                 (list (insert-txn txn (first sexp))))
          (vector? sexp) (vec (cons (insert-txn txn (first sexp)) (insert-txn txn (rest sexp))))
          (transactional-fn? (first sexp)) `(-> ~txn ~(cons (first sexp) (insert-txn txn (rest sexp))))          
          :else (cons (insert-txn txn (first sexp)) (insert-txn txn (rest sexp))))
    sexp))

(defmacro with-transcation [& body]
  (let [txn (gensym "txn")]
  `(let [service# (get-service)
         ~txn (.beginTransaction service#)]
     (try 
      (let [ret# (do ~@(insert-txn txn body))]
        (.commit ~txn)
        ret#)
      (finally 
       (if (.isActive ~txn)
         (.rollback ~txn)))))))