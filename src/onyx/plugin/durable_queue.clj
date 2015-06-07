(ns onyx.plugin.durable-queue
  (:require [clojure.core.async :refer [timeout <!!]]
            [onyx.peer.pipeline-extensions :as p-ext]
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

(defn inject-connection [event lifecycle]
  (let [task (:onyx.core/task-map event)
        opts (collect-durable-queue-opts task)]
    {:durable-queue/conn (d/queues (:durable-queue/directory task) opts)
     :durable-queue/queue-name (:durable-queue/queue-name task)}))

(defn inject-pending-state [event lifecycle]
  {:durable-queue/pending-messages (atom {})})

(defmethod p-ext/read-batch :durable-queue/read-from-queue
  [{:keys [onyx.core/task-map durable-queue/conn
           durable-queue/pending-messages durable-queue/queue-name]}]
  (let [pending (count @pending-messages)
        max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (arg-or-default :onyx/batch-timeout task-map)
        step-ms (/ ms (:onyx/batch-size task-map))
        batch (if (pos? max-segments)
                (loop [segments [] cnt 0]
                  (if (= cnt max-segments)
                    segments
                    (if-let [message (d/take! conn queue-name step-ms nil)]
                      (recur (conj segments 
                                   {:id (java.util.UUID/randomUUID)
                                    :input :durable-queue
                                    :message message})
                             (inc cnt))
                      segments)))
                (<!! (timeout step-ms)))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (:message m)))
    {:onyx.core/batch (map #(update-in % [:message] deref) batch)}))

(defmethod p-ext/ack-message :durable-queue/read-from-queue
  [{:keys [durable-queue/pending-messages]} message-id]
  (let [msg (get @pending-messages message-id)]
    (d/complete! msg)
    (swap! pending-messages dissoc message-id)))

(defmethod p-ext/retry-message :durable-queue/read-from-queue
  [{:keys [durable-queue/pending-messages]} message-id]
  (when-let [msg (get @pending-messages message-id)]
    (d/retry! msg)
    (swap! pending-messages dissoc message-id)))

(defmethod p-ext/pending? :durable-queue/read-from-queue
  [{:keys [durable-queue/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? :durable-queue/read-from-queue
  [{:keys [durable-queue/conn durable-queue/pending-messages] :as event}]
  (let [stats (get (d/stats conn) (str (:durable-queue/queue-name event)))
        state @pending-messages]
    (and (= (count state) 1)
         (= @(second (first state)) :done)
         (= (:in-progress stats) 1))))

(defmethod p-ext/write-batch :durable-queue/write-to-queue
  [{:keys [onyx.core/results durable-queue/conn durable-queue/queue-name]}]
  (doseq [msg (mapcat :leaves results)]
    (d/put! conn queue-name (:message msg)))
  {})

(defmethod p-ext/seal-resource :durable-queue/write-to-queue
  [{:keys [durable-queue/conn durable-queue/queue-name]}]
  (d/put! conn queue-name :done)
  {})

(def reader-state-calls
  {:lifecycle/before-task-start inject-pending-state})

(def reader-connection-calls
  {:lifecycle/before-task-start inject-connection})

(def writer-calls
  {:lifecycle/before-task-start inject-connection})
