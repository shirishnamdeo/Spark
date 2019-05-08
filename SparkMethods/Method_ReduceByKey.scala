https://backtobazics.com/big-data/spark/apache-spark-reducebykey-example/


Reduce is a Action Operation
ReduceByKEy is a Transformation Operation

Spark RDD reduceByKey function merges the values for each key using an associative reduce function.

ReduceByKey function works only for RDDs which contains key and value pairs kind of elements(i.e RDDs having tuple or Map as a data element)

An associative function is passed as a parameter, which will be applied to the source RDD and will create a new RDD as with resulting values(i.e. key value pair). 
This operation is a wide operation as data shuffling may happen across the partition.


The associative function (which accepts two arguments and returns a single element) should be Commutative and Associative in mathematical nature.


* Before sending data across the partitions, it also merges the data locally using the same associative function for optimized data shuffling.




Example1:

scala> val x = sc.parallelize(Array(("a", 1), ("b", 1), ("a", 1), ("a", 1), ("b", 1), ("b", 1), ("b", 1), ("b", 1)), 3)
x: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[14] at parallelize at <console>:24

scala> x.collect()
res12: Array[(String, Int)] = Array((a,1), (b,1), (a,1), (a,1), (b,1), (b,1), (b,1), (b,1))

scala> val y = x.reduceByKey((accum, n) => (accum + n))
y: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[15] at reduceByKey at <console>:25
-- SO here we don;t need to specify key. By default it takes the first element of the pair as a key and then reduce based on second parameter

scala> y.collect()
res13: Array[(String, Int)] = Array((a,3), (b,5))



Example2:

scala> val y = x.reduceByKey((a, b) => (a * b))
y: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[1] at reduceByKey at <console>:25

scala> y.collect()
res0: Array[(String, Int)] = Array((a,1), (b,1)) *** Why??





Example3:
(Short Hande Notation)

scala> val y = x.reduceByKey(_ + _)
y: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[4] at reduceByKey at <console>:25

scala> y.collect()
res1: Array[(String, Int)] = Array((a,3), (b,5))





Example4:

// Define associative function separately
scala> def sumFunc(accum:Int, n:Int) =  accum + n
sumFunc: (accum: Int, n: Int)Int

scala> x.reduceByKey(sumFunc).collect()
res2: Array[(String, Int)] = Array((a,3), (b,5))



