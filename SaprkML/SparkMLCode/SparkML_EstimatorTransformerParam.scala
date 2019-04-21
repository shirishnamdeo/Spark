[https://spark.apache.org/docs/2.4.0/ml-pipeline.html#example-estimator-transformer-and-param]


import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row



// Prepare training data from a list of (label, features) tuples.
val training = spark.createDataFrame(Seq(
    (1.0, Vectors.dense(0.0, 1.1, 0.1)),
    (0.0, Vectors.dense(2.0, 1.0, -1.0)),
    (0.0, Vectors.dense(2.0, 1.3, 1.0)),
    (1.0, Vectors.dense(0.0, 1.2, -0.5))
)).toDF("label", "features")


scala> training.show
+-----+--------------+
|label|      features|
+-----+--------------+
|  1.0| [0.0,1.1,0.1]|
|  0.0|[2.0,1.0,-1.0]|
|  0.0| [2.0,1.3,1.0]|
|  1.0|[0.0,1.2,-0.5]|
+-----+--------------+


// Create a LogisticRegression instance. This instance is an Estimator.
val lr = new LogisticRegression()

// Print out the parameters, documentation, and any default values.
println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")


// We may set parameters using setter methods.
lr.setMaxIter(10).setRegParam(0.01)

scala> lr.getMaxIter
res61: Int = 10

scala> lr.getRegParam
res62: Double = 0.01



val model1 = lr.fit(training)


// Since model1 is a Model (i.e., a Transformer produced by an Estimator), we can view the parameters it used during fit().
// This prints the parameter (name: value) pairs, where names are unique IDs for this LogisticRegression instance.
println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap}")
Model 1 was fit using parameters: {
        logreg_12dde3c6288e-aggregationDepth: 2,
        logreg_12dde3c6288e-elasticNetParam: 0.0,
        logreg_12dde3c6288e-family: auto,
        logreg_12dde3c6288e-featuresCol: features,
        logreg_12dde3c6288e-fitIntercept: true,
        logreg_12dde3c6288e-labelCol: label,
        logreg_12dde3c6288e-maxIter: 10,
        logreg_12dde3c6288e-predictionCol: prediction,
        logreg_12dde3c6288e-probabilityCol: probability,
        logreg_12dde3c6288e-rawPredictionCol: rawPrediction,
        logreg_12dde3c6288e-regParam: 0.01,
        logreg_12dde3c6288e-standardization: true,
        logreg_12dde3c6288e-threshold: 0.5,
        logreg_12dde3c6288e-tol: 1.0E-6
}



// We may alternatively specify parameters using a ParamMap, which supports several methods for specifying parameters.
val paramMap = ParamMap(lr.maxIter -> 20).
    put(lr.maxIter, 30).  // Specify 1 Param. This overwrites the original maxIter.
    put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.


// One can also combine ParamMaps.
val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
val paramMapCombined = paramMap ++ paramMap2



// Now learn a new model using the paramMapCombined parameters. paramMapCombined overrides all parameters set earlier via lr.set* methods.
val model2 = lr.fit(training, paramMapCombined)
println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap}")
Model 2 was fit using parameters: {
        logreg_12dde3c6288e-aggregationDepth: 2,
        logreg_12dde3c6288e-elasticNetParam: 0.0,
        logreg_12dde3c6288e-family: auto,
        logreg_12dde3c6288e-featuresCol: features,
        logreg_12dde3c6288e-fitIntercept: true,
        logreg_12dde3c6288e-labelCol: label,
        logreg_12dde3c6288e-maxIter: 30,
        logreg_12dde3c6288e-predictionCol: prediction,
        logreg_12dde3c6288e-probabilityCol: myProbability,
        logreg_12dde3c6288e-rawPredictionCol: rawPrediction,
        logreg_12dde3c6288e-regParam: 0.1,
        logreg_12dde3c6288e-standardization: true,
        logreg_12dde3c6288e-threshold: 0.55,
        logreg_12dde3c6288e-tol: 1.0E-6
}




// Prepare test data.
val test = spark.createDataFrame(Seq(
    (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
    (0.0, Vectors.dense(3.0, 2.0, -0.1)),
    (1.0, Vectors.dense(0.0, 2.2, -1.5))
)).toDF("label", "features")



// Make predictions on test data using the Transformer.transform() method.
// LogisticRegression.transform will only use the 'features' column.
// Note that model2.transform() outputs a 'myProbability' column instead of the usual 'probability' column since we renamed the lr.probabilityCol 
// parameter previously.
model2.transform(test).
    select("features", "label", "myProbability", "prediction").
    collect().
    foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
    }

([-1.0,1.5,1.3], 1.0) -> prob=[0.05707304171034024,0.9429269582896597], prediction=1.0
([3.0,2.0,-0.1], 0.0) -> prob=[0.9238522311704105,0.07614776882958946], prediction=0.0
([0.0,2.2,-1.5], 1.0) -> prob=[0.10972776114779474,0.8902722388522052], prediction=1.0



