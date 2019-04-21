[https://stackoverflow.com/questions/36696326/map-vs-mapvalues-in-spark]


mapValues is only applicable for PairRDDs, meaning RDDs of the form RDD[(A, B)]. 
In that case, mapValues operates on the value only (the second part of the tuple), while map operates on the entire record (tuple of key and value).

In other words, given f: B => C and rdd: RDD[(A, B)], these below two are identical (almost):

val result: RDD[(A, C)] = rdd.map { case (k, v) => (k, f(v)) }
val result: RDD[(A, C)] = rdd.mapValues(f)

The latter is simply shorter and clearer, so when you just want to transform the values and keep the keys as-is, it's recommended to use mapValues.

On the other hand, if you want to transform the keys too (e.g. you want to apply f: (A, B) => C), you simply can't use mapValues because it would 
only pass the values to your function.

The last difference concerns partitioning: if you applied any custom partitioning to your RDD (e.g. using partitionBy), using map would "forget" 
that paritioner (the result will revert to default partitioning) as the keys might have changed; mapValues, however, preserves any partitioner set 
on the RDD.

_____________________________________________________________________________________________________________________________________________________

mapValues -- Apply finction to each value of a PairRDD without changing the Key



spark.sparkContext.parallelize(List((0, 0), (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (7, 70), (8, 80), (9, 90)))

// Just forming some randon PairRDD
scala> spark.sparkContext.parallelize(List((0, 0), (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (7, 70), (8, 80), (9, 90))).map(item => (if (item._1 % 2 == 0) {item._1 / 2 } else {item._1}, item._2)).collect()
res126: Array[(Int, Int)] = Array((0,0), (1,10), (1,20), (3,30), (2,40), (5,50), (3,60), (7,70), (4,80), (9,90))


// Just forming some randon PairRDD
scala> val x = spark.sparkContext.parallelize(List((0, 0), (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (7, 70), (8, 80), (9, 90))).map(item => (if (item._1 % 2 == 0) {item._1 / 2 } else {item._1}, item._2))


scala> x.collect()
res135: Array[(Int, Int)] = Array((0,0), (1,10), (1,20), (3,30), (2,40), (5,50), (3,60), (7,70), (4,80), (9,90))



scala> x.mapValues(_ + 1)
// res136: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[141] at mapValues at <console>:26

scala> x.mapValues(_ + 1).collect()
res137: Array[(Int, Int)] = Array((0,1), (1,11), (1,21), (3,31), (2,41), (5,51), (3,61), (7,71), (4,81), (9,91))

scala> x.mapValues(i => i+1).collect()
res138: Array[(Int, Int)] = Array((0,1), (1,11), (1,21), (3,31), (2,41), (5,51), (3,61), (7,71), (4,81), (9,91))

scala> x.mapValues((_, 1)).collect()
res70: Array[(Int, (Int, Int))] = Array((0,(0,1)), (1,(10,1)), (1,(20,1)), (3,(30,1)), (2,(40,1)), (5,(50,1)), (3,(60,1)), (7,(70,1)), (4,(80,1)), (9,(90,1)))

scala> x.mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).collect()
res71: Array[(Int, (Int, Int))] = Array((0,(0,1)), (1,(30,2)), (9,(90,1)), (2,(40,1)), (3,(90,2)), (4,(80,1)), (5,(50,1)), (7,(70,1)))



_____________________________________________________________________________________________________________________________________________________

Example:

val sales = sc.parallelize( Seq( ("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7)))

scala> sales.mapValues((1,_)).collect()
// res38: Array[(String, (Int, Int))] = Array((idA,(1,2)), (idA,(1,4)), (idB,(1,3)), (idB,(1,5)), (idB,(1,10)), (idC,(1,7)))


val counts = sales.mapValues((1,_)).reduceByKey {case (a,b) => ((a._1+b._1),(a._2+b._2))}
// Above, a and b are not of the same touple, these are the different touples of the group. 
// This a._1 mean the first element of the tuple which is 1, and a._2 means the second value of the tuple, the value itself.

scala> counts.collect().foreach(println)
(idC,(1,7))
(idA,(2,6))
(idB,(3,18))


_____________________________________________________________________________________________________________________________________________________



