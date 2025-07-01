(ns functional-db.util)

"useful functions for functional-db"

(defn always [&] true)

(defn ref?
  [attr]
  (= :db/ref (:type (meta attr))))

(defn single? [attr] (= :db/single (:cardinality (meta attr))))

(defn collify [x] (if (coll? x) x [x]))
