(defproject org.onyxplatform/onyx-durable-queue "0.7.4-SNAPSHOT"
  :description "Onyx plugin for Factual's durable-queue"
  :url "https://github.com/onyx-platform/onyx-durable-queue"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.7.4-20150919_224118-gb747e06"]
                 [factual/durable-queue "0.1.5"]]
  :profiles {:dev {:dependencies [[midje "1.7.0"]]
                   :plugins [[lein-midje "3.1.1"]]}})
