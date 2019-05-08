https://backtobazics.com/big-data/spark/apache-spark-aggregatebykey-example/


AggregateByKey would be a better alternative of GroupByKey transformation when aggregation operation is involved.



Any RDD with key-value pair data is refereed as PairRDD in Spark. 
For any transformation on PairRDD, the initial step is grouping values with respect to a common key.
Spark aggregateByKey function aggregates the values of each key, using given combine functions and a neutral “zero value”.



aggregateByKey function in Spark accepts total 3 parameters,

	Initial value or Zero value. It can be 0 if aggregation is type of sum of all values/
		We have have this value as Double.MaxValue if aggregation objective is to find minimum value
		We can also use Double.MinValue value if aggregation objective is to find maximum value
		Or we can also have an empty List or Map object, if we just want a respective collection as an output for each key
		-- It is an accumulation value or initial value of an accumulator
	
	Sequence operation function which transforms/merges data of one type [V] to another type [U]
	-- This argument seqOp is an operation which aggregates all values of a single partition.
	
	Combination operation function which merges multiple transformed type [U] to a single type [U]
	-- This argument combOp is similar as seqOp and which further aggregates all aggregated values from different partitions.




Example1:
// Creating PairRDD studentRDD with key value pairs
val studentRDD = sc.parallelize(Array(
    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82), 
    ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), 
    ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87), 
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74), 
    ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), 
    ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83), 
    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)), 3)


//Defining Seqencial Operation and Combiner Operations

//Sequence operation : Finding Maximum Marks from a single partition
def seqOp = (accumulator: Int, element: (String, Int)) => 
    if(accumulator > element._2) accumulator else element._2
 
//Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
def combOp = (accumulator1: Int, accumulator2: Int) => 
    if(accumulator1 > accumulator2) accumulator1 else accumulator2


//Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
val zeroVal = 0
val aggrRDD = studentRDD.map(t => (t._1, (t._2, t._3))).aggregateByKey(zeroVal)(seqOp, combOp) 



scala> aggrRDD.collect()
res6: Array[(String, Int)] = Array((Tina,87), (Thomas,93), (Jackeline,86), (Joseph,91), (Juan,69), (Jimmy,97), (Cory,71))


scala> aggrRDD.collect foreach println
(Tina,87)
(Thomas,93)
(Jackeline,86)
(Joseph,91)
(Juan,69)
(Jimmy,97)
(Cory,71)








Example2:
Let's Print Subject name along with Maximum Marks //

//Defining Seqencial Operation and Combiner Operations
def seqOp = (accumulator: (String, Int), element: (String, Int)) => 
    if(accumulator._2 > element._2) accumulator else element
	-- So here rather than just stoging the max value we are also storing the subject which corresponds to the max value for each Student(key)


def combOp = (accumulator1: (String, Int), accumulator2: (String, Int)) => 
    if(accumulator1._2 > accumulator2._2) accumulator1 else accumulator2
 
//Zero Value: Zero value in our case will be tuple with blank subject name and 0
val zeroVal = ("", 0)
 
val aggrRDD = studentRDD.map(t => (t._1, (t._2, t._3))).aggregateByKey(zeroVal)(seqOp, combOp) 


scala> aggrRDD.collect foreach println
(Tina,(Biology,87))
(Thomas,(Physics,93))
(Jackeline,(Maths,86))
(Joseph,(Chemistry,91))
(Juan,(Physics,69))
(Jimmy,(Chemistry,97))
(Cory,(Chemistry,71))






Example3:
Printing over all percentage of all students.

//Defining Seqencial Operation and Combiner Operations
def seqOp = (accumulator: (Int, Int), element: (String, Int)) => 
    (accumulator._1 + element._2, accumulator._2 + 1)
 
def combOp = (accumulator1: (Int, Int), accumulator2: (Int, Int)) => 
    (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)
 
//Zero Value: Zero value in our case will be tuple with blank subject name and 0
val zeroVal = (0, 0)
 
// Here aggregateByKey() will return seperate counts of total marks and total subject
// So we need to convert it in a percentage value by using a separate map() function 
val aggrRDD = studentRDD.map(t => (t._1, (t._2, t._3))).aggregateByKey(zeroVal)(seqOp, combOp).map(t => (t._1, t._2._1/t._2._2*1.0))
 

scala> aggrRDD.collect foreach println
(Tina,76.0)
(Thomas,86.0)
(Jackeline,76.0)
(Joseph,82.0)
(Juan,64.0)
(Jimmy,77.0)
(Cory,65.0)






