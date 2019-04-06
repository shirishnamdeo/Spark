https://github.com/h2oai/h2o-tutorials/blob/master/tutorials/h2o-and-databricks/H2OWorld-Demo-Example.scal
a

import org.apache.spark.storage.StorageLevel


// Loading Titanic Data

val data_frame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .format("csv")
    .load("file:///D:/NotebookShare/Material/Hadoop/Spark/SparkDatasets/TitanicData/train.csv")

data_frame.persist(StorageLevel.MEMORY_AND_DISK)
data_frame.count()  //-- 891


data_frame.printSchema


count_distinct(data_frame).show()
//-- Processing take a lot of stages.
+-----------+--------------+
|column_name|distinct_count|
+-----------+--------------+
|PassengerId|           891|
|       Name|           891|
|     Ticket|           681|
|     Pclass|             3|
|      Parch|             7|
|   Embarked|             4|
|        Age|            89|
|      Cabin|           148|
|       Fare|           248|
|      SibSp|             7|
|   Survived|             2|
|        Sex|             2|
+-----------+--------------+


count_null(data_frame).show()
+-----------+----------+
|column_name|null_count|
+-----------+----------+
|PassengerId|         0|
|       Name|         0|
|     Ticket|         0|
|     Pclass|         0|
|      Parch|         0|
|   Embarked|         2|
|        Age|       177|
|      Cabin|       687|
|       Fare|         0|
|      SibSp|         0|
|   Survived|         0|
|        Sex|         0|
+-----------+----------+


count_nan(data_frame).show()
count_empty_string(data_frame).show()
// -- No count in the avove two operations


data_frame.columns
res13: Array[String] = Array(PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)



// Extracting all the Not-Null Columns
data_frame.columns.map(column_name => col(column_name).isNotNull)
res15: Array[org.apache.spark.sql.Column] = Array((PassengerId IS NOT NULL), (Survived IS NOT NULL), (Pclass IS NOT NULL), (Name IS NOT NULL), (Sex IS NOT NULL), (Age IS NOT NULL), (SibSp IS NOT NULL), (Parch IS NOT NULL), (Ticket IS NOT NULL), (Fare IS NOT NULL), (Cabin IS NOT NULL), (Embarked IS NOT NULL))


val filterCond = data_frame.columns.map(column_name => col(column_name).isNotNull).reduce(_ && _)
res28: org.apache.spark.sql.Column = ((((((((((((PassengerId IS NOT NULL) AND (Survived IS NOT NULL)) AND (Pclass IS NOT NULL)) AND (Name IS NOT NULL)) AND (Sex IS NOT NULL)) AND (Age IS NOT NULL)) AND (SibSp IS NOT NULL)) AND (Parch IS NOT NULL)) AND (Ticket IS NOT NULL)) AND (Fare IS NOT NULL)) AND (Cabin IS NOT NULL)) AND (Embarked IS NOT NULL))


data_frame.filter(filterCond).columns
res29: Array[String] = Array(PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)


data_frame.filter(filterCond).count()
res38: Long = 183 // -- All the rows which contrains any null are dropped from the data_frame 



count_null(data_frame.drop("Age", "Cabin", "Embarked")).show()

val data_frame_without_null = data_frame.drop("Age", "Cabin", "Embarked")


// Converting into H2O Frame
import org.apache.spark.h2o._
val h2oContext = H2OContext.getOrCreate(spark) 

import h2oContext._ 
import h2oContext.implicits._


// Implicit call of H2OContext.asH2OFrame(srdd) is used
val h2o_frame: H2OFrame = data_frame_without_null

// Explicit call of H2Context API with name for resulting H2O frame
val h2oframe: H2OFrame = h2oContext.asH2OFrame(data_frame_without_null)
val h2oframe: H2OFrame = h2oContext.asH2OFrame(data_frame_without_null, Some("h2oframe"))


h2oframe.numCols    // -- 9
h2oframe.numRows    // -- 891
h2oframe.names      // -- res9: Array[String] = Array(PassengerId, Survived, Pclass, Name, Sex, SibSp, Parch, Ticket, Fare)
h2oframe.means      // -- res10: Array[Double] = Array(446.0, 0.3838383838383838, 2.3086419753086447, NaN, NaN, 0.5230078563411893, 0.3815937149270483, NaN, 32.20420796857465)
h2oframe.naCount    // -- 0


import _root_.hex.tree.gbm.GBMModel
import _root_.water.support.ModelMetricsSupport


import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.VectorIndexer


