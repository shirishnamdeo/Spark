
Spark RDD reduce function reduces the elements of this RDD using the specified commutative and associative binary operator..
Commulative Property -> Order of elements does NOT matters.  a+b+c = a+c+b = b+c+a
Associative Property -> Grouping of elements does NOT matters. (a+b)+c = a+(b+c)


Spark reduce operation is almost similar as reduce method in Scala.
Reduce is an action operation of RDD which means it will trigger all the lined up transformation on the base RDD (or in the DAG) WHICH ARE NOT EXECUTED* and than execute the action operation on the last RDD.
Reduce is also a WIDE operation. In the sense the execution of this operation results in distributing the data across the multiple partitions.
-- Why wide operation? I mean we don't need the data to be shuffeled across partitions, only data need to be transfered to a centralized node (might be to a driver)
-- I think only when the reduces data would not fit into a sinlge machine, then we need more than one nodes for the final stage of reduce operations.


* It accepts a function with (which accepts two arguments and returns a single element) which should be Commutative and Associative in mathematical nature. 
* That intuitively means, this function produces same result when repetitively applied on same set of RDD data with multiple partitions irrespective of element’s order.





Example1:

scala> val x = sc.parallelize(1 to 10, 2)
x: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[13] at parallelize at <console>:24

scala> x.collect
res10: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

scala> x.reduce((a, b) => a+b)
res9: Int = 55



Example2:
(Shorter Syntax)

scala> x.reduce(_ + _)
res11: Int = 55




Example3:

scala> val y = x.reduce(_ * _)
y: Int = 3628800

