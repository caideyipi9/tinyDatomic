(ns functional-db.storage)

;; Protocol defining storage operations for entities
(defprotocol Storage
  (get-entity   [storage ent-id] "Retrieve entity by ID")
  (write-entity [storage ent]    "Add or update entity in storage")
  (drop-entity  [storage ent]    "Remove entity from storage"))

;; In-memory implementation of the Storage protocol
;; storage is a map where keys are entity IDs and values are entities
(defrecord InMemory []
  Storage
  (get-entity   [storage ent-id] (ent-id storage))  ; Direct map lookup
  (write-entity [storage ent]    (assoc storage (:id ent) ent))  ; Add/overwrite by ID
  (drop-entity  [storage ent]    (dissoc storage (:id ent))))  ; Remove by ID