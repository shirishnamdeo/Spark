
DataFrame.schema(DataFrame.schema.fieldIndex("vectorAssemblesOutputColumnName")).metadata
// Json Output of the metadata, regarding wihch index is assigned to which column name of the DataFrame while assembling

// One of the thing observed that during 



import org.apache.spark.ml.linalg.Vector

val sparseToDense = udf((v: Vector) => v.toDense)

DataFrame.withColumn("denseColumnName", sparseToDense($"sparseColumnName"))



val DataFrame = new VectorAssembler().setInputCols(Array("columnName1", "columnName2")).setOutputCol("outputColName")
// Here, the columnName1, and columnName2 can iteself be the column containing the sparse vector, which could have formed from previous VectorAssembler operation


// Converting target Column (or any String column btw), to the indexed column
val labelIndexer = new StringIndexer().setInputCol("categoricalLabelColumnName").setOutputCol("labeledOutputColumnName").fit(DataFrame)

labelIndexer.labels
// This will contains all the final label to which the dataFrame 


// Dismantling the Array[DataFrame] using below syntax
val Array(train_df, test_df) = DataFrame.randomSplit(Array(0.7, 0.3), seed=42)
.randomSplit(), takes an array which is not just limited to two elements, neither the condition of the summation to 1
We can pass an Array like, Array(0.1, 0.1, 0.1, 0.1) also to get 4 split array of 10 percentage data in each.




// Converting Index Labels back to the Categorical Labels

import org.apache.spark.ml.feature.IndexToString

val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictionLabel").setLabels(labelIndexer.labels)
val predictionDataFrame_withLabel = labelConverter.transform(predictionDataFrame)




import org.apache.spark.ml.evaluation.MultiClassClassificationEvaluator

val accuracyEvaluator = new MultiClassClassificationEvaluator().setLabelCol("targetColumn").setPredictionCol("predictionColumn").setMatricName("accuracy")
accuracyEvaluator.transform(predictionDataFrame)

[https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/evaluation/BinaryClassificationEvaluator.scala#L46]
val accuracyEvaluator = new MultiClassClassificationEvaluator().setLabelCol("targetColumn").setPredictionCol("predictionColumn").setMatricName("areaUnderROC")
accuracyEvaluator.transform(predictionDataFrame)



import org.apache.spark.mllib.evaluation.MultiClassMetrics
// It needs a PairRDD with each pair element in (prediction, lable) format.

// Method_LabelPrediction_RDD utility function
val multiCM = new MultiClassMetrics(predictionRDD_with_PredictionLabelFormat)

// The default toString method (which is called when you use println is executed), doesn't display all the row/columns.
// We need to explicitly use the other toString method which takes max lines and max width as parameter


multiCM.confusionMatrix.toString(50, Int.MaxValue)
// Returns a Matrix, with predicted classes in Columns



multiCM.accuracy
multiCM.fMeasure
multiCM.precisionmultiCM.recall
multiCM.weightedPrecision

multiCM.falsePositiveRate(classLabel)
multiCM.truePositiveRate(classLabel)

multiCM.labels



import org.apache.spark.sql.functions.skewness
import org.apache.spark.sql.functions.kurtosis

DataFrame.select(skewness("<column_name>"))
DataFrame.select(kurtosis("<column_name>"))





model.depth
model.extractParamMap

model.rootNode

