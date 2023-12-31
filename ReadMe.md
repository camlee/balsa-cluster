Cluster subclasses do not need to follow the Liskov Substitution Principle: they can extend and add as needed.
Inheritence is used here to easily gain all parent functionality.

The main method to run a cluster instance is `run()`. No matter the class, this does all the background
work (tasks) required. For flexibility, the different pieces of background work are split into individual
functions/tasks which can be overriden individuall or orchestrated specially.

| Class           | Inherits From | Used For                                   | Provides these tasks. | Provides these attributes. | And these.                   |
|-----------------|---------------|--------------------------------------------|-----------------------|----------------------------|------------------------------|
| BaseCluster     |               | Running                                    | run()                 |                            |                              |
| Cluster         | BaseCluster   | Listening and Connecting, sending objects. | listen(), connect()   | nodes, clients             |                              |
| DispatchCluster | Cluster       | Routing dict messages to methods by type.  | dispatch_messages()   |                            | @handler function decorator. |
| GossipCluster   | Cluster       | Synchronizing cluster state.               | various gossip_*      |                            |                              |