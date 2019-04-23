https://backtobazics.com/big-data/apache-spark-combinebykey-example/


Spark combineByKey is a transformation operation on PairRDD.
It is a wider operation as it requires shuffle in the last stage.


Spark combineByKey is a generic function to combine the elements for each key using a custom set of aggregation functions.
-- How it is different than reduceByKey and aggragateByKey **??


Internally spark combineByKey function efficiently combines the values of a PairRDD partition by applying aggregation function. 
The main objective of combineByKey transformation is transforming any PairRDD[(K,V)] to the RDD[(K,C)] where C is the result of any aggregation of all values under key K.


Spark combineByKey function uses following three functions as an argument,
	createCombiner:
		This function is a first argument of combineByKey function
		It is a first aggregation step for each key
		It will be executed when any new key is found in a partition
		Execution of this lambda function is local to a partition of a node, on each individual values

		--This function is similar to first argument (i.e. zeroVal) of aggregateByKey transformation.


	mergeValue:
		Second function executes when next subsequent value is given to combiner
		It also executes locally on each partition of a node and combines all values
		Arguments of this function are a accumulator and a new value
		It combines a new value in existing accumulator

		-- This function is similar to second argument (i.e. seqOp) of aggregateByKey transformation.


	mergeCombiners:
		Final function is used to combine how to merge two accumulators (i.e. combiners) of a single key across the partitions to generate final expected result
		Arguments are two accumulators (i.e. combiners)
		Merge results of a single key from different partitions

		-- This function is similar to third argument (i.e. combOp) of aggregateByKey transformation.


-- Use combineByKey when return type differs than source type (i.e. when you cannot use reduceByKey )




Difference between aggregateByKey and combineByKey:-
Spark combineByKey is a general transformation whereas internal implementation of transformations groupByKey, reduceByKey and aggregateByKey uses combineByKey.

aggregateByKey:
	Map Side Combine

combineByKey:
	Can perform Map Side and Reduce Side Combine
	














Example1:
// Creating PairRDD studentRDD with key value pairs
val studentRDD = sc.parallelize(Array(
    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), 
    ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), 
    ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78), 
    ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87), 
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), 
    ("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65), 
    ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86), 
    ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83), 
    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), 
    ("Juan", "Biology", 60)), 3)


//Defining createCombiner, mergeValue and mergeCombiner functions
def createCombiner = (tuple: (String, Int)) => 
    (tuple._2.toDouble, 1)
    
def mergeValue = (accumulator: (Double, Int), element: (String, Int)) => 
    (accumulator._1 + element._2, accumulator._2 + 1)
    
def mergeCombiner = (accumulator1: (Double, Int), accumulator2: (Double, Int)) => 
    (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)
 
 
// use combineByKey for finding percentage
val combRDD = studentRDD.map(t => (t._1, (t._2, t._3))).combineByKey(createCombiner, mergeValue, mergeCombiner).map(e => (e._1, e._2._1/e._2._2))
 

scala> combRDD.collect foreach println
(Tina,76.5)
(Thomas,86.25)
(Jackeline,76.5)
(Joseph,82.5)
(Juan,64.0)
(Jimmy,77.0)
(Cory,65.0)




