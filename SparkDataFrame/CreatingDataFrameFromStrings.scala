1. Converting the COLLECTION of String to DataFrame
    
2. Also demonstrated how to convert COLLECTION of Strings to RDD of tuples & CaseClass Object
   This part is very useful as we previously struggled to convert Arrya[Arrya[Any]] or Arrya[Arrya[String]] to DataFrame, so this might help.

_____________________________________________________________________________________________________________________________________________________


// DataFrame from collection of Strings

val stringData = Seq(
    "7369, SMITH, CLERK, 7902, 17-Dec-80, 800, 20, 10",
    "7499, ALLEN, SALESMAN, 7698, 20-Feb-81, 1600, 300, 30",
    "7521, WARD, SALESMAN, 7698, 22-Feb-81, 1250, 500, 30",
    "7566, JONES, MANAGER, 7839, 2-Apr-81, 2975, 0, 20",
    "7654, MARTIN, SALESMAN, 7698, 28-Sep-81, 1250, 1400, 30"
)


import spark.implicits._
// By importing Spark Implicits, a lot of methods will be available for converting common Scala objects into DataFrames


import org.apache.spark.sql.Row


// Cleansing the data
val splitAndStrip = (line: String) => {
    val rec = line.split(",").map(x => x.trim)
    Row(rec(0).toInt, rec(1), rec(2), rec(3).toInt, rec(4), rec(5).toDouble, rec(6).toDouble, rec(7).toInt)
}
// Can we do this dynamically ***



// Method1: N Tuple
val colList = List("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

val dataFrame1 = spark.sparkContext.parallelize(stringData, 4).
    map(splitAndStrip).
    map{
        case Row(empno: Int,ename: String,job: String,mgr: Int,hiredate: String,sal: Double,comm: Double,deptno: Int) =>
                    (empno, ename, job, mgr, hiredate, sal, comm, deptno)
    }.toDF(colList: _*)


scala> dataFrame1.show()
+-----+------+--------+----+---------+------+------+------+
|empno| ename|     job| mgr| hiredate|   sal|  comm|deptno|
+-----+------+--------+----+---------+------+------+------+
| 7369| SMITH|   CLERK|7902|17-Dec-80| 800.0|  20.0|    10|
| 7499| ALLEN|SALESMAN|7698|20-Feb-81|1600.0| 300.0|    30|
| 7521|  WARD|SALESMAN|7698|22-Feb-81|1250.0| 500.0|    30|
| 7566| JONES| MANAGER|7839| 2-Apr-81|2975.0|   0.0|    20|
| 7654|MARTIN|SALESMAN|7698|28-Sep-81|1250.0|1400.0|    30|
+-----+------+--------+----+---------+------+------+------+



// Methds2 Case Class
case class MyCaseClass(empno: Int, ename: String, job: String, mgr: Int, hiredate: String, sal: Double, comm: Double, deptno: Int)

val dataFrame2 = spark.sparkContext.parallelize(stringData, 4).
    map(splitAndStrip).
    map{
        case Row(empno: Int,ename: String, job: String, mgr: Int, hiredate: String, sal: Double, comm: Double, deptno: Int) =>
                    MyCaseClass(empno, ename, job, mgr, hiredate, sal, comm, deptno)
  }.toDF(colList: _*)

