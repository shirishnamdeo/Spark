1. First Abstraction -> RDD
2. Second Abstraction -> Shared Varaibles

RDDs are created by starting with a file in the Hadoop file system (or any other Hadoop-supported file system), or an existing SCALA COLLECTION in the driver program, and transforming it.
-- Note, that a Collection is Scala/Java specific object, which is later converted into Spark RDD, say with parallelize() method.


Shared Varaibles can be used in parallel operation.
By default, when Spark runs a function in parallel as a set of tasks on different nodes, it ships a copy of each variable used in the function to each task. 
Sometimes, a variable needs to be shared across tasks, or between tasks and the driver program. 

Spark supports two types of shared variables: broadcast variables, which can be used to cache a value in memory on all nodes, and accumulators, which are variables that are only 'added' to, such as counters and sums.


2 ways of creating RDD(S).
There are two ways to create RDDs: parallelizing an existing collection (Java/Scala/Pyhton) in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.






