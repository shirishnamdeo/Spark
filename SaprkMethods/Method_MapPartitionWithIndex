[https://supergloo.com/spark-scala/apache-spark-examples-of-transformations/#mapPartitions]

// Similar to mapPartitions but also provides a function with an Int value to indicate the index position of the partition.
// mapPartitionsWithIndex is similar to mapPartitions() but it provides second parameter index which keeps the track of partition


val data = sc.parallelize(1 to 9)

data.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", "+x).iterator).collect
Array[String] = Array(0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 7, 9)



val data2 = sc.parallelize(1 to 9, 3)

data2.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", "+x).iterator).collect
Array[String] = Array(0, 1, 0, 2, 0, 3, 1, 4, 1, 5, 1, 6, 2, 7, 2, 8, 2, 9)
-- Output Changes drastically!!




[http://apachesparkbook.blogspot.com/2015/11/mappartition-example.html]
Example

val data = spark.sparkContext.parallelize(1 to 10, 4)
data.collect().foreach(println(_))

data.toDebugString

data.mapPartitionsWithIndex((index, iterator) => iterator)
// mapPartitionsWithIndex method accepts an function object and returns a iterator


data.mapPartitionsWithIndex((index, iterator) => { 
	println("Called in Partition -> " + index)
	iterator
	}).collect()

Called in Partition -> 0
Called in Partition -> 2
Called in Partition -> 3
Called in Partition -> 1
res16: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)





// Converting the iterator (over elements in that partition to List, do the transformaton(s) and at last again return the iterator)
data.mapPartitionsWithIndex((index, iterator) => { 
	println("Called in Partition -> " + index)

	val myList = iterator.toList
	myList.map(x => x + " -> " + index).iterator

	}).collect()


Called in Partition -> 3
Called in Partition -> 1
Called in Partition -> 2
Called in Partition -> 0
res20: Array[String] = Array(1 -> 0, 2 -> 0, 3 -> 1, 4 -> 1, 5 -> 1, 6 -> 2, 7 -> 2, 8 -> 3, 9 -> 3, 10 -> 3)



