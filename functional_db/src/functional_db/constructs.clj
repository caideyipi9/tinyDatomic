(ns functional-db.constructs
  (:require [functional-db.util :as util] 
            [functional-db.storage :as storage])
  (:import [functional-db.storage InMemory]))

"define the main database structure, without the actual mechanism of dynamic entity changes and index construction"

(defrecord Database [layers top-id curr-time])  ; Database structure with layers(a vector of layer), top ID, and current time
(defrecord Layer [storage VAET AVET VEAT EAVT]) ; storage and index structures
; structure for [Entity, Attr, Value,Timestamp]
(defrecord Entity [id attrs])
(defrecord Attr [name value ts prev-ts])

(defn make-entity
  ([] (make-entity :db/no-id-yet))
  ([ent-id] (Entity. ent-id {})))

(defn make-attr
  ([name value type & {:keys [cardinality] :or {cardinality :db/single}}]
   {:pre [(contains? #{:db/single :db/multiple} cardinality)]}
   (with-meta (Attr. name value -1 -1)
     {:type type :cardinality cardinality})))

(defn add-attr
  [ent attr]
  (let [attr-name (keyword (:name attr))]
    (assoc-in ent [:attrs attr-name] attr)))

(defn make-index
  [from-eav to-eav usage-pred]
  (with-meta {}
    {:from-eav from-eav
     :to-eav to-eav
     :usage-pred usage-pred}))

(defn from-eav
  [index]
  (:from-eav (meta index)))

(defn to-eav
  [index]
  (:to-eav (meta index)))

(defn usage-pred
  [index]
  (:usage-pred (meta index)))

(defn indices [] [:VAET :AVET :VEAT :EAVT])

;; #(expr) == (fn [& x] (expr x))

(defn make-db
  "Create an empty database"
  []
  (atom (Database. [(Layer.
                     (InMemory.) ; storage
                     (make-index #(vector %3 %2 %1) #(vector %3 %2 %1) #(util/ref? %)) ; VAET - for graph queries and joins
                     (make-index #(vector %2 %3 %1) #(vector %3 %1 %2) util/always) ; AVET - for filtering
                     (make-index #(vector %3 %1 %2) #(vector %2 %3 %1) util/always) ; VEAT - for filtering
                     (make-index #(vector %1 %2 %3) #(vector %1 %2 %3) util/always))] ; EAVT - for filtering
                   0 0)))

;; Get a specific ent
(defn entity-at
  ([db ent-id] (entity-at db (:curr-time db) ent-id))
  ([db ts ent-id] (storage/get-entity (get-in db [:layers ts :storage]) ; 得到layers-根据ts键得到layer-得到storage
                                      ent-id)))

;; Get a specific attribute of an entity at a specific time(name, value, timestamp)
(defn attr-at
  ([db ent-id attr-name] (attr-at db ent-id attr-name (:curr-time db)))
  ([db ent-id attr-name ts]
   (get-in (entity-at db ts ent-id) 
           [:attrs attr-name])))


;; as above, but only value
(defn value-of-at
  ([db ent-id attr-name]  (:value (attr-at db ent-id attr-name)))
  ([db ent-id attr-name ts] (:value (attr-at db ent-id attr-name ts))))

;; get a index map
(defn indx-at
  ([db kind]
   (indx-at db kind  (:curr-time db)))
  ([db kind ts]
   (kind ((:layers db) ts))))