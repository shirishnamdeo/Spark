
// It is possible to create the DataFrame from RDD, but you need to 
// RDD.toDF(<column-list>)

_____________________________________________________________________________________________________________________________________________________


1. Creating DataFrame from Seq of Tuples (then to RDD of Tuples)

// create an RDD of tuples with some data
val seqOfTuples = Seq(
    (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
    (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
    (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
    (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20)
)
 
val rddOfTuples = spark.sparkContext.parallelize(seqOfTuples, 4)
rddOfTuples: org.apache.spark.rdd.RDD[(Int, String, String, Int, String, Int, Int, Int)]

 
// Convert RDD-of-tuples to DataFrame by supplying column names
val dataFrame1 = rddOfTuples.toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

dataFrame1.printSchema()

scala> dataFrame1.show()
+-----+-----+--------+----+---------+----+----+------+
|empno|ename|     job| mgr| hiredate| sal|comm|deptno|
+-----+-----+--------+----+---------+----+----+------+
| 7369|SMITH|   CLERK|7902|17-Dec-80| 800|  20|    10|
| 7499|ALLEN|SALESMAN|7698|20-Feb-81|1600| 300|    30|
| 7521| WARD|SALESMAN|7698|22-Feb-81|1250| 500|    30|
| 7566|JONES| MANAGER|7839| 2-Apr-81|2975|   0|    20|
+-----+-----+--------+----+---------+----+----+------+


_____________________________________________________________________________________________________________________________________________________

