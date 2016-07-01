(ns onyx.tasks.durable-queue
  (:require [schema.core :as s]
            [onyx.schema :as os]))

(def DurableQueueInputTaskMap
  {:durable-queue/queue-name s/Str
   :durable-queue/directory s/Str
   (s/optional-key :durable-queue/max-queue-size) s/Num
   (s/optional-key :durable-queue/slab-size) s/Num
   (s/optional-key :durable-queue/fsync-put?) s/Bool
   (s/optional-key :durable-queue/fsync-take?) s/Bool
   (s/optional-key :durable-queue/fsync-threshold) (s/maybe s/Num)
   (s/optional-key :durable-queue/fsync-interval) (s/maybe s/Num)
   (s/optional-key :onyx/max-peers) (s/eq 1)
   (os/restricted-ns :durable-queue) s/Any})

(s/defn ^:always-validate read-from-queue
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.durable-queue/read-from-queue
                             :onyx/type :input
                             :onyx/medium :durable-queue
                             :durable-queue/max-queue-size Integer/MAX_VALUE
                             :durable-queue/slab-size 67108864
                             :durable-queue/fsync-put? true
                             :durable-queue/fsync-take? false
                             :durable-queue/fsync-threshold nil
                             :durable-queue/fsync-interval nil
                             :onyx/max-peers 1
                             :onyx/doc "Reads segments via durable-queue"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.durable-queue/reader-state-calls}
                        {:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.durable-queue/reader-connection-calls}]}
    :schema {:task-map DurableQueueInputTaskMap}})
  ([task-name :- s/Keyword
    queue-name :- s/Str
    directory :- s/Str
    task-opts :- {s/Any s/Any}]
   (read-from-queue task-name (merge {:durable-queue/queue-name queue-name
                                      :durable-queue/directory directory}
                                     task-opts))))

(def DurableQueueOutputTaskMap
  {:durable-queue/queue-name s/Str
   :durable-queue/directory s/Str
   (s/optional-key :durable-queue/max-queue-size) s/Num
   (s/optional-key :durable-queue/slab-size) s/Num
   (s/optional-key :durable-queue/fsync-put?) s/Bool
   (s/optional-key :durable-queue/fsync-take?) s/Bool
   (s/optional-key :durable-queue/fsync-threshold) (s/maybe s/Num)
   (s/optional-key :durable-queue/fsync-interval) (s/maybe s/Num)
   (os/restricted-ns :durable-queue) s/Any})

(s/defn ^:always-validate write-to-queue
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.durable-queue/write-to-queue
                             :onyx/type :output
                             :onyx/medium :durable-queue
                             :durable-queue/max-queue-size Integer/MAX_VALUE
                             :durable-queue/slab-size 67108864
                             :durable-queue/fsync-put? true
                             :durable-queue/fsync-take? false
                             :durable-queue/fsync-threshold nil
                             :durable-queue/fsync-interval nil
                             :onyx/doc "Writes segments via durable-queue"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.durable-queue/writer-calls}]}
    :schema {:task-map DurableQueueOutputTaskMap}})
  ([task-name :- s/Keyword
    queue-name :- s/Str
    directory :- s/Str
    task-opts :- {s/Any s/Any}]
   (write-to-queue task-name (merge {}
                                    task-opts))))
