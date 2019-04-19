

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


scala> rdd_from_seq.lookup("math")
res49: Seq[Int] = WrappedArray(55, 56)
// Note the return type
