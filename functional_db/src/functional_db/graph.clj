(ns functional-db.graph
  (:require [functional-db.constructs :as constructs]
            [functional-db.util :as util]))

" Api lib1 : 
  - incoming-refs: 获取指定时间点的实体 ent-id 的所有引用实体集合
  - outgoing-refs: 获取指定时间点的实体 ent-id 的所有被引用实体集合
  - traverse-db: 从指定实体开始，遍历数据库中的实体，支持广度优先和深度优先搜索
 目前实现的功能：其实就是选定是出边还是入边遍历，选择BFS还是DFS，然后把结果放入指定容器"

; 典型的VAET应用：找到某个实体在指定时间点的所有引用实体集合
(defn incoming-refs [db ts ent-id & ref-names] 
  (let [vaet (constructs/indx-at db :VAET ts)  ; 获取指定时间的 VAET 索引
        all-attr-map (vaet ent-id)  ; 获取引用 ent-id 的所有属性映射
        filtered-map (if ref-names   ; 可选过滤特定属性
                         (select-keys ref-names all-attr-map)
                         all-attr-map)]
  (reduce into #{} (vals filtered-map))))  ; 合并所有引用实体的集合

(defn outgoing-refs [db ts ent-id & ref-names]
  (let [val-filter-fn (if ref-names #(vals (select-keys ref-names %)) vals)]
       (if-not ent-id
               []
               (->> (constructs/entity-at db ts ent-id)  ; 获取实体
                    (:attrs)                  ; 提取所有属性
                    (val-filter-fn)           ; 可选过滤特定属性
                    (filter util/ref?)        ; 仅保留引用类型的属性
                    (mapcat :value)))))       ; 提取引用的实体 ID


;; 移除已探索的实体，保持数据结构（向量或列表）
;; candidates: 待处理的实体集合
;; explored: 已探索的实体集合
;; structure-fn: 构造结果的数据结构函数（如 vec 或 list*）
;; 返回: 移除已探索实体后的数据结构
(defn- remove-explored [candidates explored structure-fn]
  (structure-fn (remove #(contains? explored %) candidates)))

;; 递归图遍历核心逻辑
;; pendings: 待处理的实体集合
;; explored: 已探索的实体集合
;; exploring-fn: 获取下一跳实体的函数
;; ent-at: 获取实体状态的函数
;; structure-fn: 构造待处理集合的数据结构函数
;; 返回: 惰性序列，包含遍历到的实体状态
(defn- traverse [pendings explored exploring-fn ent-at structure-fn]
  (let [cleaned-pendings (remove-explored pendings explored structure-fn)
        item (first cleaned-pendings)
        all-next-items  (exploring-fn item)
        next-pends (reduce conj (structure-fn (rest cleaned-pendings)) all-next-items)]
    (when item (cons  (ent-at item)
                      (lazy-seq (traverse next-pends (conj explored item) exploring-fn ent-at structure-fn))))))

;; 数据库图遍历公共接口
;; start-ent-id: 起始实体ID
;; db: 数据库实例
;; algo: 遍历算法（:graph/bfs 或 :graph/dfs）
;; direction: 遍历方向（:graph/outgoing 或 :graph/incoming）
;; ts: 可选时间戳，默认为当前时间
;; 返回: 惰性序列，包含遍历到的实体状态
(defn traverse-db
  ([start-ent-id db algo direction] (traverse-db start-ent-id db algo direction (:curr-time db)))
  ([start-ent-id db algo direction ts]
   (let [structure-fn (if (= :graph/bfs algo) vec list*)  ; BFS使用队列（向量），DFS使用栈（列表）
         explore-fn (if (= :graph/outgoing direction) outgoing-refs incoming-refs)]  ; 出边或入边遍历
     (traverse [start-ent-id] #{}  (partial explore-fn db ts) (partial constructs/indx-at db ts) structure-fn))))