[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.classification.DecisionTreeClassifier]


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}


val data = spark.read.format("libsvm").load("file:///D:/SoftwareInstalled/Spark/Spark240/spark-2.4.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt")

scala> data.printSchema
root
 |-- label: double (nullable = true)
 |-- features: vector (nullable = true)


scala> data.printSchema
root
 |-- label: double (nullable = true)
 |-- features: vector (nullable = true)


scala> data.show(5)
+-----+--------------------+
|label|            features|
+-----+--------------------+
|  0.0|(692,[127,128,129..]|
|  1.0|(692,[158,159,160..]|
|  1.0|(692,[124,125,126..]|
|  1.0|(692,[152,153,154..]|
|  1.0|(692,[151,152,153..]|
+-----+--------------------+
only showing top 5 rows



scala> data.select($"label").distinct.count
res10: Long = 2


scala> data.select($"label").groupBy("label").agg(count("label")).show()
+-----+------------+
|label|count(label)|
+-----+------------+
|  0.0|          43|
|  1.0|          57|
+-----+------------+


// Split the data into training and test sets (30% held out for testing).
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))





// Train a DecisionTree model.
val dt = new DecisionTreeClassifier()
  .setLabelCol("indexedLabel")  -- The target column name, explicitly specigied, default "label"
  .setFeaturesCol("indexedFeatures")  -- You can explicitly set the name of input features name, default "features"




