https://backtobazics.com/big-data/spark/apache-spark-flatmap-example/


FlatMap is very similar to Map RDD Operation.
FlatMap is a transformation operation which accepts a function as an aggument (similar to Map Operation)
Similar to Map, this functions is applied to each elements of the source RDD and creates a New RDD with transformed values.
As a final result it flattens all the elements of the resulting RDD in case individual elements are in form of list, array, sequence or any such collection.


FlatMap is a narrow operation as it is not shuffling data from one partition to multiple partitions.
Output of flatMap is flatten
FlatMap parameter function should return array, list or sequence (any subtype of scala.TraversableOnce)




scala> val x = sc.parallelize(List("spark rdd example",  "sample example"), 2)
x: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[7] at parallelize at <console>:24

scala> val y = x.map(x => x.split(" "))
y: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[8] at map at <console>:25
-- map operation will return Array of Arrays in following case
-- split(" ") returns an array of words


scala> y.collect()
res43: Array[Array[String]] = Array(Array(spark, rdd, example), Array(sample, example))




// flatMap operation will return Array of words in following case : Check type of res1
scala> val y = x.flatMap(x => x.split(" "))
y: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[9] at flatMap at <console>:25

scala> y.collect()
res44: Array[String] = Array(spark, rdd, example, sample, example)


val changeFlatMap = changeFileLower.flatMap("[a-z]+".r findAllIn _)