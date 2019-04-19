// Consider mapPartitionsa tool for performance optimization. 
// Now, it won’t do much good for you when running examples on your local machine, but don’t forget it when running on a Spark cluster. 
// It’s the same as map but works with Spark RDD partitions.  

// mapPartitions - Is a transformation
// mapPartitions() can be used as an alternative to map() and foreach()
// mapPartitions() can be called for each partitions while map() and foreach() is called for each elements in an RDD


In Map, a function is applied to each and every element of an RDD and returns each and every other element of the resultant RDD. 
In mapPartitions, instead of each element, the function is applied to each partition of RDD and returns multiple elements of the resultant RDD.
In mapPartitions, the performance is improved since the object creation is eliminated for each and every element as in map transformation.


We get Iterator as an argument for mapPartition, through which we can iterate through all the elements in a Partition.

val data = spark.sparkContext.parallelize(1 to 9, 3)

scala> data.getNumPartitions
res104: Int = 3

data.mapPartitions( x => List(x.next).iterator).collect
res107: Array[Int] = Array(1, 4, 7)





// compare to the same, but with default parallelize
val data2 = spark.sparkContext.parallelize(1 to 9)

data2.mapPartitions( x => List(x.next).iterator).collect

data2.getNumPartitions
res108: Int = 8

data2.mapPartitions( x => List(x.next).iterator).collect
res106: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8)





val data3 = spark.sparkContext.parallelize(1 to 20)

scala> data3.getNumPartitions
res109: Int = 8

data3.mapPartitions( x => List(x.next).iterator).collect
res111: Array[Int] = Array(1, 3, 6, 8, 11, 13, 16, 18)
