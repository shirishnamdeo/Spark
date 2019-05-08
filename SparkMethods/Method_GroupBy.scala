
Spark groupBy example can also be compared with groupby clause of SQL. In spark, groupBy is a transformation operation.
Spark groupBy is a wide operation as it shuffles data from multiple partitions and create another RDD.
*** This operation is costly as it doesn’t use combiner local to a partition to reduce the data transfer. ***
*** Not recommended to use when you need to do further aggregation on grouped data. ***


-- Note the return value is a Array of (Key and GroupedElements_ListofItems).
-- Order of the elements within the group may not be the same when we applied it multiple times.



Example1:

val x = sc.parallelize(Array("Joseph", "Jimmy", "Tina", "Thomas", "James", "Cory", "Christine", "Jackeline", "Juan"), 3)

scala> x.groupBy(w => w.charAt(0)).collect()
res6: Array[(Char, Iterable[String])] = Array((T,CompactBuffer(Tina, Thomas)), (C,CompactBuffer(Cory, Christine)), (J,CompactBuffer(Joseph, Jimmy, James, Jackeline, Juan)))
-- Group the arrays elements based on the frist character.


scala> x.groupBy(w => w.charAt(0)).keys.collect()
res22: Array[Char] = Array(T, C, J)

scala> x.groupBy(w => w.charAt(0)).values.collect()
res24: Array[Iterable[String]] = Array(CompactBuffer(Tina, Thomas), CompactBuffer(Cory, Christine), CompactBuffer(Joseph, Jimmy, James, Jackeline, Juan))


Example2:

scala> x.groupBy(w => w.length).collect()
res7: Array[(Int, Iterable[String])] = Array((6,CompactBuffer(Joseph, Thomas)), (9,CompactBuffer(Christine, Jackeline)), (4,CompactBuffer(Tina, Cory, Juan)), (5,CompactBuffer(Jimmy, James)))
-- Group the elements based of the length of elements.


Example3:
(Short Hand Notation)

scala> x.groupBy(_.length).collect()
res8: Array[(Int, Iterable[String])] = Array((6,CompactBuffer(Joseph, Thomas)), (9,CompactBuffer(Christine, Jackeline)), (4,CompactBuffer(Tina, Cory, Juan)), (5,CompactBuffer(Jimmy, James)))




