// [http://apachesparkbook.blogspot.com/search/label/z83%7C%20Iterable]

// Multiple Pair RDDs can be combined using cogroup()


val pair_rdd1 = sc.parallelize(Seq(
		("key1", 1),
		("key2", 2),
		("key1", 3)
	)
)


val pair_rdd2 = sc.parallelize(Seq(
		("key1", 5),
		("key2", 4),
		("key4", 4)
	)
)


scala> pair_rdd1.cogroup(pair_rdd2)
// res50: org.apache.spark.rdd.RDD[(String, (Iterable[Int], Iterable[Int]))] = MapPartitionsRDD[66] at cogroup at <console>:28

scala> pair_rdd1.cogroup(pair_rdd2).collect()
// res52: Array[(String, (Iterable[Int], Iterable[Int]))] = Array((key1,(CompactBuffer(1, 3),CompactBuffer(5))), (key2,(CompactBuffer(2),CompactBuffer(4))), (key4,(CompactBuffer(),CompactBuffer(4))))
// Note how the elements of the same key is compined


scala> val grouped = pair_rdd1.cogroup(pair_rdd2)



scala> grouped.map(x => x._1).collect()
// res54: Array[String] = Array(key1, key2, key4)

scala> grouped.map(x => x._2).collect()
// res55: Array[(Iterable[Int], Iterable[Int])] = Array((CompactBuffer(1, 3),CompactBuffer(5)), (CompactBuffer(2),CompactBuffer(4)), (CompactBuffer(),CompactBuffer(4)))



scala> grouped.map(x => x._2._1).collect()
// res56: Array[Iterable[Int]] = Array(CompactBuffer(1, 3), CompactBuffer(2), CompactBuffer())

scala> grouped.map(x => x._2._2).collect()
// res57: Array[Iterable[Int]] = Array(CompactBuffer(5), CompactBuffer(4), CompactBuffer(4))




scala> grouped.map(x => x._2._1.map( _ + 1)).collect()
// res59: Array[Iterable[Int]] = Array(List(2, 4), List(3), List())

