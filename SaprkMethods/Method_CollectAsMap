

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


scala> rdd_from_seq.collectAsMap()
// res42: scala.collection.Map[String,Int] = Map(math -> 56, science -> 54, english -> 58)
//  Note that because of duplicate entries, the next pair with same key, overwrites the old map value



scala> rdd_from_seq.collectAsMap().map(println(_))
(math,56)
(science,54)
(english,58)
res47: Iterable[Unit] = ArrayBuffer((), (), ())
// Note that although the above execution doesn't gave an error, it is symtically not correct. 
// Map above returned the Iterable, with no elements, with side-effect println statement
// Instead .foreach action must have been used


scala> rdd_from_seq.collectAsMap().foreach(println(_))
(math,56)
(science,54)
(english,58)





