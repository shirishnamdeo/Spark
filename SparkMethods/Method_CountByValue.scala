
scala> val data = spark.sparkContext.parallelize(List(1,2,3,4,5,2,4,6,9,4,2,0))
// data: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[31] at parallelize at <console>:23

data.collect()
res39: Array[Int] = Array(1, 2, 3, 4, 5, 2, 4, 6, 9, 4, 2, 0)

data.countByValue
countByValue   countByValueApprox

data.countByValue()
res40: scala.collection.Map[Int,Long] = Map(0 -> 1, 5 -> 1, 1 -> 1, 6 -> 1, 9 -> 1, 2 -> 3, 3 -> 1, 4 -> 3)

// Count the occurances of the values




// If a RDD with key-value pairs is parrsed, then also countByValue will take each pair as a single value and not consider the value of the pair.

val rdd_from_seq = sc.parallelize(
	Seq(
		("math",    55),
		("math",    56),
		("english", 57),
		("english", 58),
		("science", 59),
		("science", 54)
	)
)

rdd_from_seq.countByValue()
res41: scala.collection.Map[(String, Int),Long] = Map((english,58) -> 1, (math,56) -> 1, (science,54) -> 1, (science,59) -> 1, (math,55) -> 1, 
	(english,57) -> 1)


