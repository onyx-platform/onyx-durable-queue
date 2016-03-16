(ns onyx.plugin.durable-queue-test
  (:require [aero.core :refer [read-config]]
            [onyx.plugin.durable-queue]
            [durable-queue :as d]
            [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin
             [core-async :refer [take-segments! get-core-async-channels]]
             [durable-queue]]
            [onyx.tasks
             [durable-queue :as dq]
             [core-async :as core-async]]))

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(defn build-job [input-queue-name output-queue-name dir batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:in :inc]
                                    [:inc :out]]
                         :catalog [(merge {:onyx/name :inc
                                           :onyx/fn ::my-inc
                                           :onyx/type :function}
                                          batch-settings)]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (dq/read-from-queue :in (merge {:durable-queue/queue-name input-queue-name
                                                  :durable-queue/directory dir
                                                  :durable-queue/fsync-take? true} batch-settings)))
        (add-task (dq/write-to-queue :out (merge {:durable-queue/queue-name output-queue-name
                                                  :durable-queue/directory dir
                                                  :durable-queue/fsync-put? true} batch-settings))))))

(deftest durable-queue-test
  (let [{:keys [env-config
                peer-config
                durable-queue-config]} (read-config (clojure.java.io/resource "config.edn") {:profile :test})
        {:keys [durable-queue/dir
                durable-queue/input-queue-name
                durable-queue/output-queue-name]} durable-queue-config
        job (build-job input-queue-name output-queue-name dir 10 1000)
        conn (d/queues dir {})]
    (try
      (doseq [n (range 1000)]
        (d/put! conn input-queue-name {:n n}))
      (d/put! conn input-queue-name :done)
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> (:job-id (onyx.api/submit-job peer-config job))
             (onyx.api/await-job-completion peer-config))
        (is (= (set
                (butlast
                 (loop [rets []]
                   (let [x (d/take! (d/queues dir {}) output-queue-name 1000 nil)]
                     (when x
                       (d/complete! x))
                     (cond (nil? x)
                           rets
                           (= @x :done)
                           (conj rets @x)
                           :else
                           (recur (conj rets @x)))))))
               (set (map (fn [x] {:n (inc x)}) (range 1000))))))
      (d/delete! conn))))
