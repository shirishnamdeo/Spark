1. toDF()               [From Sequence Objects]
2. createDataFrame()    [From Sequence Objects]
3. createDF()           [From Sequence Objects]  -- Not Available
4. 


_____________________________________________________________________________________________________________________________________________________

1. toDF()               [From Sequence Objects]
// toDF() provides a concise syntax for creating DataFrames and can be accessed after importing Spark implicits.


// Important Note ***
// .toDF() method works if RDD is of type RDD[INT], RDD[Long], RDD[String] and RDD[T <: scala.Product]. 
// Note: "<:"" means T must be sub type of scala.Product in which case, we can use either a Case class or a Tuple. 
// This is because, Product is the super class of both Case class and Tuple.

// .toDF works only if collection is composed of Products (i.e., case classes or tuples)

    

import spark.implicits._

val sequenceObject1 = Seq((8, "bat"), (64, "mouse"), (-27, "horse"))
// sequenceObject1: Seq[(Int, String)] = List((8,bat), (64,mouse), (-27,horse))
// Note that the schema is auto-inferred by Scala type inference

scala> sequenceObject1.toDF()
// res5: org.apache.spark.sql.DataFrame = [_1: int, _2: string]
// Spark itself assign the column names if not provded explictly

sequenceObject1.toDF("colName1", "colName2")
// res4: org.apache.spark.sql.DataFrame = [colName1: int, colName2: string]


scala> sequenceObject1.toDF("colName1", "colName2").printSchema
root
 |-- colName1: integer (nullable = false)
 |-- colName2: string (nullable = true)
// Not that the Datatype is also inferred explictly by Spark
// Note that the Integer Column is not Nullable, but the String Column is Nullable.




// Using toDF() function on the existing DataFrame to name the columns
ExistingDataFrame.toDF("colName1", "colName2", "colName3")



Limitations:
	Column type and the Nullable flag cannot be customized.
	toDF is good for local testing but not recommended for Production level code.




_____________________________________________________________________________________________________________________________________________________


2. createDataFrame()    [From Sequence Objects]



import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


val sequenceObject2 = Seq( Row(8, "bat"), Row(64, "mouse"), Row(-27, "horse"))
sequenceObject2: Seq[org.apache.spark.sql.Row] = List([8,bat], [64,mouse], [-27,horse])


// Defining the Schema with name, type and nullable parameters
val Schema1 = List(
    StructField("Number", IntegerType, true),
    StructField("Words", StringType, true)
)


// Converting Seq Objects to RDD[Row], and then creating the dataframe
spark.createDataFrame(
    spark.sparkContext.parallelize(sequenceObject2),
    StructType(Schema1)
)
// res8: org.apache.spark.sql.DataFrame = [Number: int, Words: string]



// Creating Dataframe directly from Seq Object
val dataDF = spark.createDataFrame(Seq(
    (1, 1, 2, 3, 8),
    (2, 4, 3, 8, 7),
    (3, 6, 1, 9, 2)
    ))
// dataDF: org.apache.spark.sql.DataFrame = [_1: int, _2: int ... 3 more fields]
// DataFrame is created but with _N column name
// Each tuple in the input forms a row in the DataFrame

scala> dataDF.show()
+---+---+---+---+---+
| _1| _2| _3| _4| _5|
+---+---+---+---+---+
|  1|  1|  2|  3|  8|
|  2|  4|  3|  8|  7|
|  3|  6|  1|  9|  2|
+---+---+---+---+---+



import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

val training1 = spark.createDataFrame(Seq(
  (1.0, Vectors.dense(3.0)),
  (0.0, Vectors.dense(3.0))) 
).toDF("label", "features")


scala> training1.show()
+-----+--------+
|label|features|
+-----+--------+
|  1.0|   [3.0]|
|  0.0|   [3.0]|
+-----+--------+





_____________________________________________________________________________________________________________________________________________________


[http://bailiwick.io/2017/08/08/selecting-dynamic-columns-in-spark-dataframes/]

val dataDF2 = dataDF.toDF("colToExclude", "coll", "col2", "col3", "col4")

val colsToSelect = dataDF2.columns.filter(_ != "colToExclude")
//colsToSelect: Array[String] = Array(coll, col2, col3, col4)


// Taking a look at the colsToSelect value (Converting Array[String] to a single string)
colsToSelect.mkString(",")
res66: String = coll,col2,col3,col4,col5,col6


// Selecting the Columns
// This method creates a new datafreme using your column list, Filter dataDF using the colsToSelect array, and map the results into columns.
dataDF2.select(colsToSelect.head, colsToSelect.tail: _*).show()


import org.apache.spark.sql.Column
dataDF2.select( dataDF2.columns.filter(colName => colsToSelect.contains(colName)).map(colName => new Column(colName)): _* ).show()

dataDF2.select( dataDF2.columns.filter(colName => colsToSelect.contains(colName)).map(colName => col(colName)): _* ).show()
// This will aso give the same result as above with new Column() object

scala> dataDF2.columns.filter(colName => colsToSelect.contains(colName))
// res21: Array[String] = Array(coll, col2, col3, col4)
// filter will keep only those columns which condition evaluated to 'true', else discard it. 
// [Can use this step in our genericFunction for datatypes] ***



// Note **
// In the simple selection method, note that we had to use colsToSelect.head and colsToSelect.tail: _*. 
// The reason for this is that the overloaded dataframe.select() method for multiple columns requires at least 2 column names. 
// If youjust put in the array name without using .head and .tail, you'll get an overloaded method error. [dataDF2.select(colsToSelect)]






val sequenceObject3 = Seq(
    Row(1, "col1_val1", "col2_val1", "col3_val1"),
    Row(2, "col1_val4", "col2_val4", null),
    Row(3, "",          "col2_val6", "col3_val6")
    )

val Schema3 = List(
    StructField("Number", IntegerType, true),
    StructField("Col1", StringType, true),
    StructField("Col2", StringType, true),
    StructField("Col3", StringType, true)
)
// Schema3: List[org.apache.spark.sql.types.StructField]


val dataframe3 = spark.createDataFrame(
    spark.sparkContext.parallelize(sequenceObject3),
    StructType(Schema3)
)


scala> dataframe3.show()
+------+---------+---------+---------+
|Number|     Col1|     Col2|     Col3|
+------+---------+---------+---------+
|     1|col1_val1|col2_val1|col3_val1|
|     2|col1_val4|col2_val4|     null|
|     3|         |col2_val6|col3_val6|
+------+---------+---------+---------+


scala> dataframe3.printSchema
root
 |-- Number: integer (nullable = true)
 |-- Col1: string (nullable = true)
 |-- Col2: string (nullable = true)
 |-- Col3: string (nullable = true)
// So if we provide the explicit Datatype then the DataFrame will remain that type
// I guess while creating the DataFrame with .toDF() method, it 'null' or NaN come in the Seq Object, then the type of the column change to 'Any'




_____________________________________________________________________________________________________________________________________________________

3. createDF()           [From Sequence Objects]
createDF() is defined in spark-daria and allows for the following terse syntax.

-- spark-daria **???


val someDF = spark.createDF(
  List(
    (8, "bat"),
    (64, "mouse"),
    (-27, "horse")
  ), List(
    ("number", IntegerType, true),
    ("word", StringType, true)
  )
)

ERROR: value createDF is not a member of org.apache.spark.sql.SparkSession


_____________________________________________________________________________________________________________________________________________________


4. Problems in .toDF() Method

// As mentioned above, Column Type in .toDF() method cannot be customized.

val data = Seq(
    (1, "col2_val1", null, "col4_val1", 50),
    (2, "col2_val2", 4,    "col4_val1", 12),
    (3, "col2_val3", " ",  "col4_val1", null),
    (4, null,        1234, "col4_val1", Double.NaN),
    (5, "",          null, "col4_val1", 55)
)
// data: Seq[(Int, String, Any, String, Any)] = 
//  List((1,col2_val1,null,col4_val1,50), (2,col2_val2,4,col4_val1,12), (3,col2_val3," ",col4_val1,null), (4,null,1234,col4_val1,NaN), 
//      (5,"",null,col4_val1,55))


val dataFrame = data.toDF("colName1", "colName2", "colName3", "colName4", "colName5")
// ERROR - Because now our sequence object contains Null and NaN



_____________________________________________________________________________________________________________________________________________________

5. Issue
.toDF() can only use Seq(of Tuples) to construct a proper dataframe. 
How to construct a DataFrame of Array[Array[String]]


scala> Seq(("str1", "str2", "str3"), ("str4", "str5", "str6")).toDF().show()
+----+----+----+
|  _1|  _2|  _3|
+----+----+----+
|str1|str2|str3|
|str4|str5|str6|
+----+----+----+


scala> Array(Array("str1", "str2", "str3"), Array("str4", "str5", "str6")).map(x => x.toSeq).toSeq.toDF.show()
+------------------+
|             value|
+------------------+
|[str1, str2, str3]|
|[str4, str5, str6]|
+------------------+


scala> Seq(Seq("str1", "str2", "str3"), Seq("str4", "str5", "str6")).toDF().show()
+------------------+
|             value|
+------------------+
|[str1, str2, str3]|
|[str4, str5, str6]|
+------------------+


scala> Seq(List("str1", "str2", "str3"), List("str4", "str5", "str6")).toDF().show()
+------------------+
|             value|
+------------------+
|[str1, str2, str3]|
|[str4, str5, str6]|
+------------------+


scala> List(List("str1", "str2", "str3"), List("str4", "str5", "str6")).toDF().show()
+------------------+
|             value|
+------------------+
|[str1, str2, str3]|
|[str4, str5, str6]|
+------------------+


scala> Seq(Seq("str1", "str2", "str3").toVector, Seq("str4", "str5", "str6").toVector).toDF().show()
+------------------+
|             value|
+------------------+
|[str1, str2, str3]|
|[str4, str5, str6]|
+------------------+



_____________________________________________________________________________________________________________________________________________________




