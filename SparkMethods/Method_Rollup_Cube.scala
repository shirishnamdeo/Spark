[https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-multi-dimensional-aggregation.html]


cube
	RelationalGroupedDataset
	Calculates subtotals and a grand total for every permutation of the columns specified.

rollup
	RelationalGroupedDataset
	Calculates subtotals and a grand total over (ordered) combination of groups.


_____________________________________________________________________________________________________________________________________________________

Difference between Cupe and Rollup

Cube generates a result set that shows aggregates for all combinations of value of selected columns, whereas,
Rollup generates a result set that shows aggregates for a hierarchy of values of selected columns


val df = Seq(("foo", 1L), ("foo", 2L), ("bar", 2L), ("bar", 2L)).toDF("x", "y")
scala> df.show()
+---+---+
|  x|  y|
+---+---+
|foo|  1|
|foo|  2|
|bar|  2|
|bar|  2|
+---+---+


scala> df.cube("x", "y").count.show
+----+----+-----+
|   x|   y|count|
+----+----+-----+
| bar|   2|    2|	<- count of records where x = bar AND y = 2
|null|null|    4|	<- total count of records
| foo|   2|    1|	<- count of records where x= foo and y = 2
|null|   1|    1|	<- count of records where y = 1
| foo|null|    2|	<- count of records where x = foo
| foo|   1|    1|	<- count of records where x= foo and y = 1
|null|   2|    3|	<- count of records where y = 2
| bar|null|    2|	<- count of records where x = bar
+----+----+-----+


// A similar function to cube is rollup which computes hierarchical subtotals from left to right:
scala> df.rollup("x", "y").count.show
+----+----+-----+
|   x|   y|count|
+----+----+-----+
| bar|   2|    2|
|null|null|    4|
| foo|   2|    1|
| foo|null|    2|
| foo|   1|    1|
| bar|null|    2|
+----+----+-----+



ROLLUP(col1, col2, col2) [hierchy is assumbed from left-to-right]
Then the result will be 
	1. Grouping by all the three columns (col1, col2, col3)
+	2. Grouping by both the columns (col1, col2)
+ 	3. Grouping by (col1)
+ 	4. ()

Thes why why in the above code we have 
(foo, 1)
(foo, 2)
(bar, 2)
(foo, null) -> Grouping by just one column (col1), thus null in column y appears. 
(bar, null)
(null, null)


Note that null is not used in grouping, it is just present to emphasize that the other column is absent while grouping data.


While in CUBE, grouping is done on all the possible combination avaible/can be formed.





_____________________________________________________________________________________________________________________________________________________




val sales = Seq(
  ("Warsaw", 2016, 100),
  ("Warsaw", 2017, 200),
  ("Boston", 2015, 50),
  ("Boston", 2016, 150),
  ("Toronto", 2017, 50)
).toDF("city", "year", "amount")


scala> sales.show()
+-------+----+------+
|   city|year|amount|
+-------+----+------+
| Warsaw|2016|   100|
| Warsaw|2017|   200|
| Boston|2015|    50|
| Boston|2016|   150|
|Toronto|2017|    50|
+-------+----+------+


val groupByCityAndYear = sales.groupBy("city", "year").agg(sum("amount") as "amount")

scala> groupByCityAndYear.show()
+-------+----+------+
|   city|year|amount|
+-------+----+------+
|Toronto|2017|    50|
| Boston|2015|    50|
| Warsaw|2016|   100|
| Boston|2016|   150|
| Warsaw|2017|   200|
+-------+----+------+


val groupByCityOnly = sales.groupBy("city").agg(sum("amount") as "amount").select($"city", lit(null) as "year", $"amount")

scala> groupByCityOnly.show()
+-------+----+------+
|   city|year|amount|
+-------+----+------+
|Toronto|null|    50|
| Warsaw|null|   300|
| Boston|null|   200|
+-------+----+------+



scala> groupByCityAndYear.union(groupByCityOnly).show()
+-------+----+------+
|   city|year|amount|
+-------+----+------+
|Toronto|2017|    50|
| Boston|2015|    50|
| Warsaw|2016|   100|
| Boston|2016|   150|
| Warsaw|2017|   200|
|Toronto|null|    50|
| Warsaw|null|   300|
| Boston|null|   200|
+-------+----+------+


val withUnion = groupByCityAndYear.union(groupByCityOnly).sort($"city".desc_nulls_last, $"year".asc_nulls_last)
// city value descreasing with nulls at last, and for same city with year values increasing and nulls in last

scala> withUnion.show()
+-------+----+------+
|   city|year|amount|
+-------+----+------+
| Warsaw|2016|   100|
| Warsaw|2017|   200|
| Warsaw|null|   300|
|Toronto|2017|    50|
|Toronto|null|    50|
| Boston|2015|    50|
| Boston|2016|   150|
| Boston|null|   200|
+-------+----+------+



val withRollup = sales.
	rollup("city", "year").
	agg(sum("amount") as "amount", grouping_id() as "gid").
	sort($"city".desc_nulls_last, $"year".asc_nulls_last).
	filter(grouping_id() =!= 3).
	select("city", "year", "amount")

scala> withRollup.show()
+-------+----+------+
|   city|year|amount|
+-------+----+------+
| Warsaw|2016|   100|
| Warsaw|2017|   200|
| Warsaw|null|   300|
|Toronto|2017|    50|
|Toronto|null|    50|
| Boston|2015|    50|
| Boston|2016|   150|
| Boston|null|   200|
+-------+----+------+



scala> sales.show()
+-------+----+------+
|   city|year|amount|
+-------+----+------+
| Warsaw|2016|   100|
| Warsaw|2017|   200|
| Boston|2015|    50|
| Boston|2016|   150|
|Toronto|2017|    50|
+-------+----+------+


scala> sales.rollup("city", "year").agg(sum("amount") as "amount", grouping_id() as "gid").show()
+-------+----+------+---+
|   city|year|amount|gid|
+-------+----+------+---+
|   null|null|   550|  3|
| Warsaw|2016|   100|  0|
|Toronto|null|    50|  1|
| Boston|2016|   150|  0|
| Warsaw|2017|   200|  0|
|Toronto|2017|    50|  0|
| Boston|2015|    50|  0|
| Boston|null|   200|  1|
| Warsaw|null|   300|  1|
+-------+----+------+---+




_____________________________________________________________________________________________________________________________________________________

Rollup Example:

val inventory = Seq(
  ("table", "blue", 124),
  ("table", "red", 223),
  ("chair", "blue", 101),
  ("chair", "red", 210)).toDF("item", "color", "quantity")

scala> inventory.show
+-----+-----+--------+
| item|color|quantity|
+-----+-----+--------+
|chair| blue|     101|
|chair|  red|     210|
|table| blue|     124|
|table|  red|     223|
+-----+-----+--------+

// ordering and empty rows done manually for demo purposes
scala> inventory.rollup("item", "color").sum().show
+-----+-----+-------------+
| item|color|sum(quantity)|
+-----+-----+-------------+
|chair| blue|          101|
|chair|  red|          210|
|chair| null|          311|
|     |     |             |
|table| blue|          124|
|table|  red|          223|
|table| null|          347|
|     |     |             |
| null| null|          658|
+-----+-----+-------------+


_____________________________________________________________________________________________________________________________________________________