
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


scala> rdd_from_seq.collect()
res32: Array[(String, Int)] = Array((math,55), (math,56), (english,57), (english,58), (science,59), (science,54))


scala> rdd_from_seq.countByKey()
res37: scala.collection.Map[String,Long] = Map(math -> 2, english -> 2, science -> 2)


