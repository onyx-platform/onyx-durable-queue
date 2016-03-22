(ns onyx.plugin.durable-queue
  (:require [clojure.core.async :refer [timeout <!!]]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.types :as t]
            [onyx.peer.function :as function]
            [onyx.static.default-vals :refer [arg-or-default]]
            [durable-queue :as d]))

(defn collect-durable-queue-opts [task-map]
  (select-keys
   task-map
   [:durable-queue/max-queue-size
    :durable-queue/slab-size
    :durable-queue/fsync-put?
    :durable-queue/fsync-take?
    :durable-queue/fsync-threshold
    :durable-queue/fsync-interval]))

(defn inject-connection [{:keys [onyx.core/pipeline]} lifecycle]
  (let [conn (:conn pipeline)
        queue-name (:queue-name pipeline)]
    {:durable-queue/conn conn 
     :durable-queue/queue-name queue-name}))

(defn inject-pending-state [event lifecycle]
  {:durable-queue/pending-messages (:pending-messages (:onyx.core/pipeline event))})

(defrecord DurableQueueReader [max-pending batch-size batch-timeout 
                               pending-messages
                               conn
                               queue-name]
  p-ext/Pipeline
  (write-batch 
    [this event]
    (function/write-batch event))

  (read-batch [_ event]
    (let [pending (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          finish-time (+ (System/currentTimeMillis) batch-timeout)
          batch (if (pos? max-segments)
                  (loop [segments [] cnt 0 segment-timeout batch-timeout]
                    (if (or (= cnt max-segments) (<= segment-timeout 0))
                      segments
                      (let [start-time (System/currentTimeMillis)] 
                        (if-let [message (d/take! conn queue-name segment-timeout nil)]
                          (recur (conj segments 
                                       (t/input (java.util.UUID/randomUUID) message))
                                 (inc cnt)
                                 (- segment-timeout (- (System/currentTimeMillis) start-time)))
                          segments))))
                  (<!! (timeout batch-timeout)))]
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) (:message m)))
      {:onyx.core/batch (map #(update-in % [:message] deref) batch)}))

  p-ext/PipelineInput

  (ack-segment [_ _ segment-id]
    (let [msg (get @pending-messages segment-id)]
      (d/complete! msg)
      (swap! pending-messages dissoc segment-id)))

  (retry-segment 
    [_ _ segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (d/retry! msg)
      (swap! pending-messages dissoc segment-id)))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained? 
    [_ event]
    (let [stats (get (d/stats conn) (str queue-name))
          state @pending-messages]
      (and (= (count state) 1)
           (= @(second (first state)) :done)
           (= (:in-progress stats) 1)))))

(defn read-from-queue [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        max-pending (arg-or-default :onyx/max-pending catalog-entry)
        batch-size (:onyx/batch-size catalog-entry)
        batch-timeout (arg-or-default :onyx/batch-timeout catalog-entry)
        pending-messages (atom {})
        opts (collect-durable-queue-opts catalog-entry)
        conn (d/queues (:durable-queue/directory catalog-entry) opts)
        queue-name (:durable-queue/queue-name catalog-entry)]
    (->DurableQueueReader max-pending batch-size batch-timeout 
                          pending-messages conn queue-name)))

(defrecord DurableQueueWriter [conn queue-name]
  p-ext/Pipeline
  (read-batch 
    [_ event]
    (function/read-batch event))

  (write-batch 
    [_ {:keys [onyx.core/results]}]
    (doseq [msg (mapcat :leaves (:tree results))]
      (d/put! conn queue-name (:message msg)))  
    {:durable-queue/written? true})

  (seal-resource 
    [_ {:keys [onyx.core/results]}]
    (d/put! conn queue-name :done)
    {}))

(defn write-to-queue [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        opts (collect-durable-queue-opts catalog-entry)
        conn (d/queues (:durable-queue/directory catalog-entry) opts)
        queue-name (:durable-queue/queue-name catalog-entry)]
    (->DurableQueueWriter conn queue-name)))

(def reader-state-calls
  {:lifecycle/before-task-start inject-pending-state})

(def reader-connection-calls
  {:lifecycle/before-task-start inject-connection})

(def writer-calls
  {:lifecycle/before-task-start inject-connection})
