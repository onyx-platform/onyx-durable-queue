## onyx-durable-queue

Onyx plugin for Factual's [durable-queue](https://github.com/Factual/durable-queue).

#### Installation

In your project file:

```clojure
[onyx-durable-queue "0.6.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.durable-queue])
```

#### Functions

##### read-from-queue

Catalog entry:

```clojure
{:onyx/name :read-from-queue
 :onyx/ident :durable-queue/read-from-queue
 :onyx/type :input
 :onyx/medium :durable-queue
 :durable-queue/queue-name input-queue-name
 :durable-queue/directory queue-directory
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Reads segments via durable-queue"}
```

Lifecycle entries:

```clojure
[{:lifecycle/task :read-from-queue
  :lifecycle/calls :onyx.plugin.durable-queue/reader-state-calls}
 {:lifecycle/task :read-from-queue
  :lifecycle/calls :onyx.plugin.durable-queue/reader-connection-calls}]
```

##### write-to-queue

Catalog entry:

```clojure
{:onyx/name :write-to-queue
 :onyx/ident :durable-queue/write-to-queue
 :onyx/type :output
 :onyx/medium :durable-queue
 :durable-queue/queue-name output-queue-name
 :durable-queue/directory queue-directory
 :onyx/batch-size batch-size
 :onyx/doc "Writes segments via durable-queue"}
```

Lifecycle entries:

```clojure
[{:lifecycle/task :write-to-queue
  :lifecycle/calls :onyx.plugin.durable-queue/writer-calls}]
```

#### Attributes

All attributes correspond directly to the durable-queue configuration opts. See [their documentation](https://github.com/Factual/durable-queue) for descriptions of each.

|key                             | type      |
|--------------------------------|-----------|
|`:durable-queue/queue-name`     | `string`  |
|`:durable-queue/directory`      | `string`  |
|`:durable-queue/max-queue-size` | `int`     |
|`:durable-queue/slab-size`      | `int`     |
|`:durable-queue/fsync-put?`     | `boolean` |
|`:durable-queue/fsync-take?`    | `boolean` |
|`:durable-queue/fsync-threshold`| `int`     |
|`:durable-queue/fsync-interval` | `int`     |

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
