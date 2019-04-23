
Spark RDD filter function returns a new RDD containing only the elements that satisfy a predicate.
Filter is a transformation operation which accepts a predicate as an argument.
Predicate is function which accepts some parameter and returns boolean value true or false. 

Filter is a narrow operation as it is not shuffling data from one partition to multiple partitions.


Example1:

scala> val x = sc.parallelize(1 to 10, 2)
x: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> x.partitions.size
res0: Int = 2

scala> x.filter(i => i%2==0).collect()
res1: Array[Int] = Array(2, 4, 6, 8, 10)





Example2:
(Short hand notation)

scala> val y = x.filter(_ % 2 == 0).collect()
y: Array[Int] = Array(2, 4, 6, 8, 10)


