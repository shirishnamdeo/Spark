import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

val dtreeClassifier = new DecisionTreeClassifier().setLabelCol("explicitNameGiven").setFeatureCol("nameOfFeatureColumn")

val trained_model = dtreeClassifier.fit(DataFrame)
val predictionDataFrame = trained_model.transform(DataFrame)




trained_model.featureImportances
trained_model.featureImportances.size
trained_model.featureImportances.toDense

