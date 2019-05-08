

_____________________________________________________________________________________________________________________________________________________

***

Why are the curly brackets used and not the regular parentheses in the map?
[Code from CreatingDataFrameFromStrings.scala]

val dataFrame1 = spark.sparkContext.parallelize(stringData, 4).
    map(splitAndStrip).
    map{
        case Row(empno: Int,ename: String,job: String,mgr: Int,hiredate: String,sal: Double,comm: Double,deptno: Int) =>
                    (empno, ename, job, mgr, hiredate, sal, comm, deptno)
    }.toDF(colList: _*)


Ans: 
	In general, there are many cases where we should prefer curly braces (e.g. multiline expressions, for comprehensions) etc. 
	In this case it’s not just curly braces, instead of parentheses, it’s curly braces excluding omited parentheses. 
	The curly braces here allows us to write pattern matching anonymous function inside map/filter, whereas parenthesis won’t.

_____________________________________________________________________________________________________________________________________________________



Map is a transformation operation in Spark hence it is lazily evaluated
Map accepts a function argument, and apply this function to each element of the Source RDD elements.
Map is a narrow operation as it is not shuffling data from one partition to multiple partitions


Example1:

scala> val x = sc.parallelize(List("spark", "rdd", "example",  "sample", "example"), 3)
x: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[10] at parallelize at <console>:24

scala> x.map(w => (w, 1))
res45: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[11] at map at <console>:26

scala> x.map(w => (w, 1)).collect()
res46: Array[(String, Int)] = Array((spark,1), (rdd,1), (example,1), (sample,1), (example,1))





Example2:
(Shorthand way of writing the above code, _ is used as a placeholder of each iterative element)

scala> val y = x.map((_, 1))
y: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[13] at map at <console>:25

scala> y.collect
res47: Array[(String, Int)] = Array((spark,1), (rdd,1), (example,1), (sample,1), (example,1))


x.map(_).collect()       -- Simple underscore doesn't work
x.map((_)).collect()     -- Simple underscore doesn't work
x.map(_ => _).collect()  -- Simple underscore doesn't work





Example3:

scala> val y = x.map(x => (x, x.length))
y: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[14] at map at <console>:25

scala> y.collect()
res48: Array[(String, Int)] = Array((spark,5), (rdd,3), (example,7), (sample,6), (example,7))




Example4:

scala> val x = sc.parallelize(List("spark rdd example",  "sample example"), 2)
x: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> x.partitions.size
res1: Int = 2

scala> x.count
res2: Long = 2


scala> x.map(l => l.split(" "))
res11: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[4] at map at <console>:26
-- MapPartitionsRDD[4] Why 4 elements.
-- Its not a RDD with 4 elements, but it is just the incremental number assigned on spark-shell.


scala> x.map(l => l.split(" ")).foreach(w => println(w))
[Ljava.lang.String;@1d5bcb17
[Ljava.lang.String;@29a12996


scala> val y = x.map(l => l.split(" "))
y: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[6] at map at <console>:25
-- y is an RDD of Array[String], but I cannot reference it like y(0). How to get elements of it then?

scala> y.count()
res17: Long = 2

scala> y.collect()
res22: Array[Array[String]] = Array(Array(spark, rdd, example), Array(sample, example))


scala> y.collect()(1)
res23: Array[String] = Array(sample, example)
-- So the array referencing is only possible for pure Scala objects I believe, not on Spark Context RDD's







scala> spark.sparkContext.parallelize(List("Shirish Namdeo", "Shashank Namdeo"), numSlices = 10).map(name => name.split(" ")).collect()
res54: Array[Array[String]] = Array(Array(Shirish, Namdeo), Array(Shashank, Namdeo))

scala> spark.sparkContext.parallelize(List("Shirish Namdeo", "Shashank Namdeo"), numSlices = 10).map(name => name.split(" "))
res55: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[27] at map at <console>:24

scala> spark.sparkContext.parallelize(List("Shirish Namdeo", "Shashank Namdeo"), numSlices = 10).map(name => name.split(" ")).map(item => item.length).collect()
res56: Array[Int] = Array(2, 2)

scala> spark.sparkContext.parallelize(List("Shirish Namdeo", "Shashank Namdeo"), numSlices = 10).map(name => name.split(" ")).map(item => item(1)).collect()
res57: Array[String] = Array(Namdeo, Namdeo)

scala> spark.sparkContext.parallelize(List("Shirish Namdeo", "Shashank Namdeo"), numSlices = 10).map(name => name.split(" ")).map(item => item(0)).collect()
res58: Array[String] = Array(Shirish, Shashank)

scala> spark.sparkContext.parallelize(List("Shirish Namdeo", "Shashank Namdeo"), numSlices = 10).map(name => name.split(" ")).map(item => item(0).length).collect()
res59: Array[Int] = Array(7, 8)




***
The method map converts each element of the source RDD into a single element of the result RDD by applying a function. mapPartitions converts each partition of the source RDD into multiple elements of the result (possibly none).