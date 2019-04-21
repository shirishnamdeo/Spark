import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row

// Prepare training documents from a list of (id, text, label) tuples.
val training = spark.createDataFrame(Seq(
    (0L, "a b c d e spark", 1.0),
    (1L, "b d", 0.0),
    (2L, "spark f g h", 1.0),
    (3L, "hadoop mapreduce", 0.0)
)).toDF("id", "text", "label")


scala> training.show
+---+----------------+-----+
| id|            text|label|
+---+----------------+-----+
|  0| a b c d e spark|  1.0|
|  1|             b d|  0.0|
|  2|     spark f g h|  1.0|
|  3|hadoop mapreduce|  0.0|
+---+----------------+-----+


// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")

val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)

val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))




// Fit the pipeline to training documents.
val model = pipeline.fit(training)

// Now we can optionally save the fitted pipeline to disk
model.write.overwrite().save("/tmp/spark-logistic-regression-model")

// We can also save this unfit pipeline to disk
pipeline.write.overwrite().save("/tmp/unfit-lr-model")

// And load it back in during production
val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")




// Prepare test documents, which are unlabeled (id, text) tuples.
val test = spark.createDataFrame(Seq(
    (4L, "spark i j k"),
    (5L, "l m n"),
    (6L, "spark hadoop spark"),
    (7L, "apache hadoop")
)).toDF("id", "text")

scala> test.show
+---+------------------+
| id|              text|
+---+------------------+
|  4|       spark i j k|
|  5|             l m n|
|  6|spark hadoop spark|
|  7|     apache hadoop|
+---+------------------+



// Make predictions on test documents.
model.transform(test).
    select("id", "text", "probability", "prediction").
    collect().
    foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    }
// (4, spark i j k) --> prob=[0.1596407738787411,0.8403592261212589], prediction=1.0
// (5, l m n) --> prob=[0.8378325685476612,0.16216743145233883], prediction=0.0
// (6, spark hadoop spark) --> prob=[0.0692663313297627,0.9307336686702373], prediction=1.0
// (7, apache hadoop) --> prob=[0.9821575333444208,0.01784246665557917], prediction=0.0
