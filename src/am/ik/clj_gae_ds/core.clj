(ns am.ik.clj-gae-ds.core
  (:use [clojure.test]
        [clojure.contrib.singleton]
        [clojure.contrib.seq-utils])
  (:import [com.google.appengine.api.datastore 
            DatastoreServiceFactory DatastoreService 
            Entity Key KeyFactory KeyRange
            Query Query$FilterOperator Query$SortDirection 
            PreparedQuery FetchOptions FetchOptions$Builder 
            Transaction]))
(def 
 #^{:arglists '([])
    :doc "get DatastoreService. This method returns singleton instance of the service."} 
 #^DatastoreService
 get-ds-service (global-singleton #(DatastoreServiceFactory/getDatastoreService)))

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


(defn- #^String key->str 
  "convert keyword to string. if key-or-str is not clojure.lang.Keyword, 
   then use key-or-str directory."
  [key-or-str]
  (if (keyword? key-or-str) (name key-or-str) key-or-str))

;; Query
(defn #^Query query 
  "create query."
  ([kind-or-ancestor] (Query. kind-or-ancestor))
  ([kind ancestor] (Query. kind ancestor)))

(def #^{:arglists '([kind-or-ancestor] [kind ancestor])
            :doc "aliase of (query)"}
     q
     query)

(defn #^Query add-sort 
  "add sort option to query.
  ex. (add-sort (query \"Entity\") \"property\" :desc)
      -> #<Query SELECT * FROM Entity ORDER BY property DESC>
      (add-sort (query \"Entity\") \"property\" :asc)
      -> #<Query SELECT * FROM Entity ORDER BY property>" 
  [q prop-name asc-or-desc]
  (.addSort q prop-name (cond (= asc-or-desc :desc) Query$SortDirection/DESCENDING 
                             (= asc-or-desc :asc) Query$SortDirection/ASCENDING
                             :else asc-or-desc)))

(def #^{:arglists '([q prop-name asc-or-desc])
            :doc "aliase of (add-sort)"}
     srt 
     #^Query
     add-sort)

(defn #^Query add-filter
  "add filter option to query.
   operator can be Keyword (:eq, :neq, :lt, :gt, :lte, :gte, :in)
   or function (= not= > >= < <=)
   ex. (add-filter (query \"Entity\") \"property\" :gt 100)
       -> #<Query SELECT * FROM Entity WHERE property > 100>
       (add-filter (query \"Entity\") \"property\" not= 100)
       -> #<Query SELECT * FROM Entity WHERE property != 100>"
  [q prop-name operator value]
  (.addFilter q prop-name (condp = operator 
                            = Query$FilterOperator/EQUAL
                            not= Query$FilterOperator/NOT_EQUAL
                            > Query$FilterOperator/GREATER_THAN
                            >= Query$FilterOperator/GREATER_THAN_OR_EQUAL
                            < Query$FilterOperator/LESS_THAN
                            <= Query$FilterOperator/LESS_THAN_OR_EQUAL
                            :eq Query$FilterOperator/EQUAL
                            :neq Query$FilterOperator/NOT_EQUAL
                            :gt Query$FilterOperator/GREATER_THAN
                            :gte Query$FilterOperator/GREATER_THAN_OR_EQUAL
                            :lt Query$FilterOperator/LESS_THAN
                            :lte Query$FilterOperator/LESS_THAN_OR_EQUAL
                            :in Query$FilterOperator/IN
                            :else operator) value))

(def #^{:arglists '([q prop-name operator value])
        :doc "aliase of (add-filter)"}
     #^Query
     flt add-filter)

(defn #^PreparedQuery prepare 
  "parepare query."
  [#^Query q]     
  (.prepare (get-ds-service) q))

(defn query-seq   
  "return sequence made from the result of query."
  ([#^Query q]
     (lazy-seq (.asIterable (prepare q))))
  ([#^Query q fetch-options]
     (lazy-seq (.asIterable (prepare q) fetch-options))))

(defmulti count-entities "return count of entities." class)

(defmethod count-entities Query [q]
  (.countEntities (prepare q)))

(defmethod count-entities PreparedQuery [pq]
  (.countEntities pq))

;; Datestore
(defn ds-put 
  "put entity to datastore"
  ([entity-or-entities]
     (.put (get-ds-service) entity-or-entities))
  ([#^Transaction txn entity-or-entities]
     (.put (get-ds-service) txn entity-or-entities)))

(defn ds-get 
  "get entity from datastore.
   If entity is not found, return nil.
  "
  ([key-or-keys]
     (try 
      (.get (get-ds-service) key-or-keys)
      (catch Throwable e nil)))
  ([#^Transaction txn key-or-keys]
     (try (.get (get-ds-service) txn key-or-keys)
          (catch Throwable e nil))))

(defn ds-delete
  "delete entity from datastore"
  ([key-or-keys]
     (.delete (get-ds-service) (if (instance? Iterable key-or-keys) key-or-keys [key-or-keys])))
  ([#^Transaction txn key-or-keys]
     (.delete (get-ds-service) txn (if (instance? Iterable key-or-keys) key-or-keys [key-or-keys]))))

(defn #^KeyRange allocate-ids 
 ([kind num] (.allocateIds (get-ds-service) kind num))
 ([parent-key kind num] (.allocateIds (get-ds-service) parent-key kind num)))

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
  `(let [service# (get-ds-service)
         ~txn (.beginTransaction service#)]
     (try 
      (let [ret# (do ~@(insert-txn txn body))]
        (.commit ~txn)
        ret#)
      (finally 
       (if (.isActive ~txn)
         (.rollback ~txn)))))))