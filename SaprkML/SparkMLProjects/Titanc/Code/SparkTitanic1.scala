[http://blog.madhukaraphatak.com/analysing-kaggle-titanic-data/]
[https://github.com/phatak-dev/spark-ml-kaggle/blob/master/src/main/scala/com/madhukaraphatak/spark/ml/titanic/RandomForest.scala]


import org.apache.spark.sql.SparkSession
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics

_____________________________________________________________________________________________________________________________________________________

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame

object MLUtils {

  def accuracyScore(df: DataFrame, label: String, predictCol: String) = {
    val rdd = df.select(predictCol,label).rdd.map(row ⇒ (row.getDouble(0), row.getInt(1).toDouble))
    new MulticlassMetrics(rdd).accuracy
  }

  def recall(df: DataFrame, labelCol: String, predictCol: String, labelValue:Double) = {
    val rdd = df.select(predictCol,labelCol).rdd.map(row ⇒ (row.getDouble(0), row.getInt(1).toDouble))
    new MulticlassMetrics(rdd).recall(labelValue)
  }

  def trainTestSplit(df:DataFrame, testSize:Double = 0.3):(DataFrame,DataFrame) = {
    val dfs = df.randomSplit(Array(1-testSize, testSize))
    val trainDf = dfs(0)
    val crossDf = dfs(1)
    (trainDf,crossDf)
  }

}


_____________________________________________________________________________________________________________________________________________________



// Loading Data in RDD

val train_rdd = spark.sparkContext.textFile("file:///D:/NotebookShare/Material/Hadoop/Spark/SaprkML/SparkMLProjects/Titanc/Data/train.csv")
val train_df =  spark.read.format("csv").option("header", true).option("inferSchema", true).load("file:///D:/NotebookShare/Material/Hadoop/Spark/SaprkML/SparkMLProjects/Titanc/Data/train.csv")


scala> train_df.printSchema
root
 |-- PassengerId: integer (nullable = true)
 |-- Survived: integer (nullable = true)
 |-- Pclass: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- Sex: string (nullable = true)
 |-- Age: double (nullable = true)
 |-- SibSp: integer (nullable = true)
 |-- Parch: integer (nullable = true)
 |-- Ticket: string (nullable = true)
 |-- Fare: double (nullable = true)
 |-- Cabin: string (nullable = true)
 |-- Embarked: string (nullable = true)



scala> count_null(train_df).show()
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


scala> count_nan(train_df).show()
scala> count_empty_string(train_df).show()
scala> count_single_space(train_df).show()
// All zeroes, thus only nulls are present in dataframe



// Filling up the missing values in the Age column with its meanValue
scala> val meanValue = train_df.agg(mean(train_df("Age"))).first.getDouble(0)
meanValue: Double = 29.69911764705882
// agg method returns a dataframe

val train_df_fixNull = train_df.na.fill(Map("Age" -> meanValue))





val splits = train_df_fixNull.randomSplit(Array(0.7, 0.3), seed=42)
// Array[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]]
// Why this randomSplit is giving different count every time. After specifying the seed, it become constant

scala> splits.size
// res1: Int = 2

val train_split_ds = splits(0).withColumnRenamed("Survived", "label")
// train_split_ds: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [PassengerId: int, Survived: int ... 10 more fields]
// DS.withColumnRenamed("Survived", "label")
// "label" is the column name needed for the Machine Learning ML model.

train_split_ds.count()
res62: Long = 620

scala> val crossVL_split_ds = splits(1)
// crossVL_split_ds: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [PassengerId: int, Survived: int ... 10 more fields]

scala> crossVL_split_ds.count()
res64: Long = 271


_____________________________________________________________________________________________________________________________________________________

// Handling Categorical Variables
// In our dataset, many columns like Sex,Embarked are categorical variables. So we are one-hot encoding them using spark ML pipeline API’s. 
// In this example, we are using StringIndexer and OneHotEncoder to do that.


def handleCategorical(column: String): Array[PipelineStage] = {
  val stringIndexer = new StringIndexer().setInputCol(column).setOutputCol(s"${column}_index").setHandleInvalid("skip")
  val oneHot = new OneHotEncoder().setInputCol(s"${column}_index").setOutputCol(s"${column}_onehot")
  Array(stringIndexer, oneHot)
}

// Create stages for all categorical variables
val genderStages = handleCategorical("Sex")
val embarkedStages = handleCategorical("Embarked")
val pClassStages = handleCategorical("Pclass")



___________________________________________________________________________________________________

scala> genderStages
// res32: Array[org.apache.spark.ml.PipelineStage] = Array(strIdx_48d8c2280d07, oneHot_85bf35593f77)
// Note the return datatype, also note that each of them is an array (which is required by the Pipeline's setStages method)

scala> genderStages(0).explainParams()
// res57: String =
// handleInvalid: 
//     How to handle invalid data (unseen labels or NULL values). Options are 'skip' (filter out rows with invalid data), error (throw an error), or 
//     'keep' (put invalid data in a special additional bucket, at index numLabels). (default: error, current: skip)

// inputCol: 
//     input column name (current: Sex)

// outputCol: 
//     output column name (default: strIdx_bab6ab8114d2__output, current: Sex_index)

// stringOrderType: 
//     How to order labels of string column. The first label after ordering is assigned an index of 0. 
//     Supported options: frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc. (default: frequencyDesc)



scala> (genderStages(0), genderStages(1))
// res35: (org.apache.spark.ml.PipelineStage, org.apache.spark.ml.PipelineStage) = (strIdx_48d8c2280d07,oneHot_85bf35593f77)

genderStages(0).fit(train_split_ds)


val column = "Sex"
val stringIndexer_dummy = new StringIndexer().setInputCol(column).setOutputCol(s"${column}_index").setHandleInvalid("skip")
// res48: org.apache.spark.ml.feature.StringIndexer = strIdx_ccd5c88be596
// The above code works, but why genderStages(0) is not a StringIndexer class object?? ***

stringIndexer_dummy.fit(train_split_ds).transform(train_split_ds).show()


scala> stringIndexer_dummy.asInstanceOf[PipelineStage]
// res51: org.apache.spark.ml.PipelineStage = strIdx_ccd5c88be596 -- Its because it has been casted int PipelineStage object.
// Can we cast it back.

scala> genderStages(0).asInstanceOf[StringIndexer]
res52: org.apache.spark.ml.feature.StringIndexer = strIdx_bab6ab8114d2

genderStages(0).asInstanceOf[StringIndexer].fit(train_split_ds).transform(train_split_ds).show 
// Perfect!!!

___________________________________________________________________________________________________




// Create ML Pipeline with RandomForest Classifier

//columns for training
val cols = Array("Sex_onehot", "Embarked_onehot", "Pclass_onehot", "SibSp", "Parch", "Age", "Fare")
val vectorAssembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")

//algorithm stage
val randomForestClassifier = new RandomForestClassifier()

//pipeline
val preProcessStages = genderStages ++ embarkedStages ++ pClassStages ++ Array(vectorAssembler)
// res73: Array[org.apache.spark.ml.PipelineStage] = Array(strIdx_078494c64b8b, oneHot_29f34fe4abe5, strIdx_28d8c525a3fe, oneHot_99645d8fc5ce, strIdx_f15353316af8, oneHot_a5d344f16f11, vecAssembler_4f6473643316)


val pipeline = new Pipeline().setStages(preProcessStages ++ Array(randomForestClassifier))
// res74: org.apache.spark.ml.Pipeline = pipeline_77518e4550ca



// Fit the Model
val model = pipeline.fit(train_split_ds)



// Accuracy Score
def accuracyScore(df: DataFrame, label: String, predictCol: String) = {
  val rdd = df.select(label, predictCol).rdd.map(row ⇒ (row.getInt(0).toDouble, row.getDouble(1)))
  new MulticlassMetrics(rdd).accuracy
}

println("train accuracy with pipeline" + accuracyScore(model.transform(trainDf), "label", "prediction"))
println("test accuracy with pipeline" + accuracyScore(model.transform(crossDf), "Survived", "prediction"))


_____________________________________________________________________________________________________________________________________________________

// Cross Validation and Hyper Parameter Tuning

// Random forest comes with many parameters which we can tune. Tuning them manually is lot of work. So we can use cross validation facility provided
// by spark ML to search through these parameter space to come up with best parameters for our data.


// Specifying the parameter grid
// The below are the parameters which we want to search for.

val paramMap = new ParamGridBuilder()
  .addGrid(randomForestClassifier.impurity, Array("gini", "entropy"))
  .addGrid(randomForestClassifier.maxDepth, Array(1,2,5, 10, 15))
  .addGrid(randomForestClassifier.minInstancesPerNode, Array(1, 2, 4,5,10))
  .build()



// Cross Validation
// Once we define the parameters, we define cross validation stage to search through these parameters. 
// Cross validation also make sures that we don’t overfit the data.

def crossValidation(pipeline: Pipeline, paramMap: Array[ParamMap], df: DataFrame): Model[_] = {
  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(new BinaryClassificationEvaluator)
    .setEstimatorParamMaps(paramMap)
    .setNumFolds(5)
  cv.fit(df)
}

val cvModel = crossValidation(pipeline, paramMap, trainDf)


// Generating Submit File
// As the data used in this example is part of kaggle competition, I generated results for their test data to sumbit using below code.

val testDf = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/titanic/test.csv")
val fareMeanValue = df.agg(mean(df("Fare"))).first.getDouble(0)
val fixedOutputDf = testDf.na.fill(meanValue, Array("age")).na.fill(fareMeanValue, Array("Fare"))

def generateOutputFile(testDF: DataFrame, model: Model[_]) = {
    val scoredDf = model.transform(testDF)
    val outputDf = scoredDf.select("PassengerId", "prediction")
    val castedDf = outputDf.select(outputDf("PassengerId"), outputDf("prediction").cast(IntegerType).as("Survived"))    
    castedDf.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save("src/main/resources/output/")
  }