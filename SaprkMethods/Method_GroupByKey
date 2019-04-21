
GroupByKey is a transformation operation and also a wider operation because it demands data shuffle.
Spark groupByKey function takes key-value pair (K,V) as an input produces RDD with key and list of values.
Spark RDD groupByKey function collects the values for each key in a form of an iterator.



GroupByKey function in Apache Spark just groups all values with respect to a single key. 
Unlike reduceByKey it doesn’t per form any operation on final output. 
It just group the data and returns in a form of an iterator.



Now because in source RDD multiple keys can be there in any partition, this function require to shuffle all data with of a same key to a single partition unless your source RDD is already partitioned by key. And this shuffling makes this transformation as a wider transformation.


It is slightly different than groupBy() transformation as it requires Key Value pair whereas in groupBy() you may or may not have keys in source RDD.
Transformation function groupBy() needs a function to form a key which is not needed in case of spark groupByKey() function.
-- That is GroupByKey functions doesn't need any function input.


GroupByKey operation is costly as it doesn’t use combiner local to a partition to reduce the data transfer *
GroupByKey always results in Hash-Partitioned RDDs.




Example1:

scala> val x = sc.parallelize(Array(("USA", 1), ("USA", 2), ("India", 1), ("UK", 1), ("India", 4), ("India", 9), ("USA", 8), ("USA", 3), ("India", 4), ("UK", 6), ("UK", 9), ("UK", 5)), 3)
x: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> x.groupByKey()
res2: org.apache.spark.rdd.RDD[(String, Iterable[Int])] = ShuffledRDD[2] at groupByKey at <console>:26

scala> x.groupByKey().collect()
res3: Array[(String, Iterable[Int])] = Array((UK,CompactBuffer(1, 6, 9, 5)), (USA,CompactBuffer(1, 2, 8, 3)), (India,CompactBuffer(1, 4, 9, 4)))



scala> val y = x.groupByKey
y: org.apache.spark.rdd.RDD[(String, Iterable[Int])] = ShuffledRDD[4] at groupByKey at <console>:25

scala> y.getNumPartitions
res4: Int = 3
-- Note that the number of partitions are equal to the number of Groups formed. But is it true always that number of partitions will be same as number of groups(give that a group fits in a partition)




Example2:

val sales = sc.parallelize( Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7) ))
val counts = sales.groupByKey().mapValues(sq => (sq.size,sq.sum)) 

scala> counts.collect().foreach(println(_))
(idC,(1,7))
(idA,(2,6))
(idB,(3,18))



