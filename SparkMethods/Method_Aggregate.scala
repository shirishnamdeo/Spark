[https://www.javaworld.com/article/3184109/aggregating-with-apache-spark.html?page=2]

[https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-aggregate-functions.html]
_____________________________________________________________________________________________________________________________________________________


val sales = Seq(("idA", 2), ("idA", 4), ("idB", 3), ("idB",5),("idB",10),("idC",7)).toDF("id", "vl")
val counts = sales.groupBy($"id").agg( count($"id"),sum($"vl"))

scala> counts.collect().foreach(println(_))
[idA,2,6]
[idB,3,18]
[idC,1,7]


scala> sales.agg(count("id"), sum("vl")).collect().foreach(println(_))
[6,31]

// What the code above did is it count the number of elements in "id" column and the sum of elements is "vl" column




DataFrame.groupBy("<column_name>").agg("<column_name>")
DataFrame.groupBy("<column_name>").agg(count("<column_name>"))


--Alising the Column Name
DataFrame.groupBy("<column_name>").agg(count("<column_name>") as "new_column_name")
DataFrame.groupBy("<column_name>").agg(count("<column_name>").as("new_column_name"))

DataFrame.select($"<column_name>".as[String])
DataFrame.select($"<column_name>".as("new_column_name"))
DataFrame.select($"<column_name>".alias("new_column_name"))
DataFrame.select($"<column_name>".alias("new_column_name").as[String])


DataFrame.groupBy("<column_name>").agg(max("<column_name>"))
-- In above query, there is only one column in groupBy clause, and we are taking the max of that group, thus only one element would be in each group




_____________________________________________________________________________________________________________________________________________________

def aggregate( an initial value)(an intra-partition sequence operation)(an inter-partition combination operation)

We start with 0 as the initial value in each of the 12 buckets. The first _+_ is the intra-partition sum, adding the total number of flowers picked by each picker in each quadrant of the garden. The second _+_ is the inter-partition sum, which aggregates the total sums from each quadrant.




val flowers = sc.parallelize(List(11,12,13,24,25, 26, 35,36,37, 24,15,16),4)
val sum = flowers.aggregate(0)(_+_, _+_)



Aggregation with accumulators
To illustrate the concept further, assume we want to find out the maximum number of flowers in each quadrant of the garden, and then aggregate the totals. We only need a slight alteration to the intra-partition function:

Listing 11. Accumulators for a quadrant

val sumofmaximums = flowers.aggregate(0)(Math.max(_,_), _+_)



And what if we wanted to find the maximum number of flowers each person could pick, across the entire garden? We could do:

Listing 12. Accumulators for the garden

val maximum = flowers.aggregate(0)(Math.max(_,_), Math.max(_,_))


The initial value used in these examples is called an accumulator, in this case, a value that is iterated across partitions and then propagated for the final result


Aggregation with tuples
For our final example, let's say that we can use as many initial values as we want. In that case, we could solve the problem of finding the average number of flowers among all of the flower pickers in each quadrant of our garden like this:

Listing 13. Tuples

val flowersandpickers = flowers.aggregate((0,0)) (
        (acc, value) => (acc._1 + value, acc_.2 +1),
         (acc1, acc2) => acc1._1 + acc2._1, acc1._2 + acc2._2)
)

In this example, notice that the reduce functions applied within the aggregate have to be both commutative and associative. There shouldn't be any order of execution for sequencing or combining operations. The initial values are two zeros, representing a tuple, or pair. The first zero is the initial value for the sum total number of flowers picked (because you start with zero flowers in the basket); the second zero is the initial value we use to find the average sum of flowers picked per picker (because you start with zero flowers picked). The intra-sequence aggregation adds the number of flowers in each quadrant. At the same time, we add the number 1, indicating that we have added one flower picker per basket. The inter-partition combination function adds the number of flowers and the number of flower pickers from each quadrant. To find the average, we then write:

Listing 14. Averaging with tuples

val avg = flowersandpickers._1/ flowersandpickers._2.toDouble




