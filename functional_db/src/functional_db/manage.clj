(ns functional-db.manage
  ;; Import database construction module
  (:require [functional-db.constructs :as constructs]))

"user's management for functional-db"

;; Private atom storing all database connections as {db-name → db-connection}
(def ^:private __ALL-DBS__ (atom {}))

;; Internal helper: Create and register a database if it doesn't exist
(defn- put-db
  [dbs db-name]
  (if (db-name dbs)
    dbs  ; Return existing registry if db exists
    (assoc dbs db-name (constructs/make-db))))  ; Create new db if not

;; Internal helper: Remove database from registry
(defn- drop-db [dbs db-name] (dissoc dbs db-name))

;; Internal helper: Convert db name to keyword for consistent indexing
(defn- as-db-name [db-name] (keyword db-name))

;; Get or create a database connection (thread-safe)
;; Returns: Database connection (atom reference)
(defn get-db-conn [db-name]
  (let [stored-db-name (as-db-name db-name)]
    ;; Atomic operation: Ensure only one thread creates a new database
    (stored-db-name (swap! __ALL-DBS__ put-db stored-db-name)))) ;key as function, get the value of stored-db-name

;; Close and remove a database connection (thread-safe)
;; Returns: nil (operation takes effect via side effect)
(defn drop-db-conn [db-name]
  (let [stored-db-name (as-db-name db-name)]
    (swap! __ALL-DBS__ drop-db stored-db-name)) nil)

;; Extract the actual database state from a connection
;; Returns: Database state (immutable data structure)
(defn db-from-conn [conn] @conn)