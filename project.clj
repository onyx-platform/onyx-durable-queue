(defproject org.onyxplatform/onyx-durable-queue "0.6.0"
  :description "Onyx plugin for Factual's durable-queue"
  :url "https://github.com/onyx-platform/onyx-durable-queue"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.onyxplatform/onyx "0.6.0"]
                 [factual/durable-queue "0.1.5"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :plugins [[lein-midje "3.1.1"]]}})
