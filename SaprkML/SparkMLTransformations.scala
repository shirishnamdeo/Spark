[https://towardsdatascience.com/feature-encoding-with-spark-2-3-0-part-1-9ede45562740]
[https://towardsdatascience.com/feature-encoding-made-simple-with-spark-2-3-0-part-2-5bfc869a809a]
[https://community.hortonworks.com/articles/75295/spark-categorical-data-transformation.html]



1. StringIndexer
2. OneHotEncoder
3. VectorAssembler
4. VectorIndexer
5. StandardScaler
6. Normalizer
7. MinMaxScaler
8. Bucketizer
9. QuantileDiscretizer

_____________________________________________________________________________________________________________________________________________________

1. String Indexer

// StringIndexer - Index categorial features

// The StringIndex function maps a string column of labels to an ML column of label indices. 
// In our example, this code will traverse through the dataframe and create a matching index for each of the values in the fruit name column.

// String Indexer encodes a column of string labels/categories to a column of indices. 
// The ordering of the indices is done on the basis of popularity and the range is [0, numOfLabels).


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.StringIndexer



// Method1 (without Pipeline)

// Just defining the indexer object. It will be applied later to the dataframe using .transform() method
val indexer = new StringIndexer()
    .setInputCol("fruit")
    .setOutputCol("fruitIndex")
    .fit(df)

scala> indexer
res86: org.apache.spark.ml.feature.StringIndexerModel = strIdx_f7684b3d3d2f

scala> indexer.labels
res88: Array[String] = Array(coconut, banana, apple)

scala> indexer.getOutputCol
res92: String = fruitIndex




val indexed = indexer.transform(df)
// That is StringIndexer class has the fit() and transform() methods.

scala> indexed.printSchema
root
 |-- id: integer (nullable = false)
 |-- fruit: string (nullable = true)
 |-- fruitIndex: double (nullable = false)


scala> indexed.show()
+---+-------+----------+
| id|  fruit|fruitIndex|
+---+-------+----------+
|  0|  apple|       2.0|
|  1| banana|       1.0|
|  2|coconut|       0.0|
|  1| banana|       1.0|
|  2|coconut|       0.0|
+---+-------+----------+

// coconut is given index 0, because it is most popular.





// Method2 (with Pipeline)

val my_pipeline = new Pipeline("my_pipeline")
// Note the uid here "my_pipeline" is Optional


val df = spark.createDataFrame(Seq(
	    (0, "apples", "red"),
	    (1, "oranges", "orange"),
	    (2, "banana", "yellow"),
	    (3, "apples", "red"),
	    (4, "banana", "yellow"),
	    (5, "coconut", "brown"),
	    (6, "oranges", "orange")
    )
).toDF("id", "category", "color")


scala> df.columns
res10: Array[String] = Array(id, category, color)

scala> df.columns.filterNot(_.contains("id"))
res9: Array[String] = Array(category, color)


val features = df.columns.filterNot(_.contains("id"))
// "id" feaute is excluded as do not want it to be indexed by StringIndexer


val encodedFeatures = features.flatMap({ name => 
	val indexer = new StringIndexer().setInputCol(name).setOutputCol(name + "_Index")	
	Array(indexer) // Return an array (although containing only one element)

}).toArray
// Why for flatMap, curly braces is needed and normal () doesn't works!!  -- I think because we are passing a function in it.
// Also I guess .toArray is not needed a flatMap will already returns an Array[org.apache.spark.ml.feature.StringIndexer] object



val pipeline = new Pipeline().setStages(encodedFeatures)

val indexer_model = pipeline.fit(df)
// res17: org.apache.spark.ml.PipelineModel = pipeline_2db0a310ac33


// Now we will apply all the transformation stages in pipeline to our dataframe
val df_transformed = indexer_model.transform(df)


scala> df_transformed.show()
+---+--------+------+--------------+-----------+
| id|category| color|category_Index|color_Index|
+---+--------+------+--------------+-----------+
|  0|  apples|   red|           0.0|        1.0|
|  1| oranges|orange|           1.0|        0.0|
|  2|  banana|yellow|           2.0|        2.0|
|  3|  apples|   red|           0.0|        1.0|
|  4|  banana|yellow|           2.0|        2.0|
|  5| coconut| brown|           3.0|        3.0|
|  6| oranges|orange|           1.0|        0.0|
+---+--------+------+--------------+-----------+


_____________________________________________________________________________________________________________________________________________________

2. OneHotEncoder

//  OneHotEncoder - Encode to one hot vectors

// One hot encoder maps the label indices to a binary vector representation with at the most a single one-value. 
// These methods are generally used when we need to use categorical features but the algorithm expects continuous features.

// The spark one hot encoder takes the indexed label/category from the string indexer and then encodes it into a sparse vector. 
// This is slightly different from the usual dummy column creation style. 


// The OneHotEncoder function maps a column of category indices to a column of binary vectors. 
// In our example, this code will convert the values into a binary vector and ensure only one of them is set to true or hot.



import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.OneHotEncoderEstimator




// Method1: (without Pipeline)

val encoder = new OneHotEncoder()
    .setInputCol("fruitIndex")
    .setOutputCol("fruitVec")
 
val encoded = encoder.transform(indexed)
encoded: org.apache.spark.sql.DataFrame = [id: int, fruit: string ... 2 more fields]

scala> encoded.printSchema
root
 |-- id: integer (nullable = false)
 |-- fruit: string (nullable = true)
 |-- fruitIndex: double (nullable = false)
 |-- fruitVec: vector (nullable = true)


scala> encoded.show()
+---+-------+----------+-------------+
| id|  fruit|fruitIndex|     fruitVec|
+---+-------+----------+-------------+
|  0|  apple|       2.0|    (2,[],[])|
|  1| banana|       1.0|(2,[1],[1.0])|
|  2|coconut|       0.0|(2,[0],[1.0])|
|  1| banana|       1.0|(2,[1],[1.0])|
|  2|coconut|       0.0|(2,[0],[1.0])|
+---+-------+----------+-------------+




// Method2 (with Pipeline)


val encodedFeatures = features.flatMap({ name => 
	val stringIndexer = new StringIndexer().setInputCol(name).setOutputCol(name + "_Index")	
	val oneHotEncoder = new OneHotEncoderEstimator().setInputCols(Array(name + "_Index")).setOutputCols(Array(name + "_vec")).setDropLast(false)
	
	Array(stringIndexer, oneHotEncoder) // Return an array (although containing only one element)

})



val pipeline = new Pipeline().setStages(encodedFeatures)
val indexer_model = pipeline.fit(df)

val df_transformed = indexer_model.transform(df)


scala> df_transformed.show()
+---+--------+------+--------------+-------------+-----------+-------------+
| id|category| color|category_Index| category_vec|color_Index|    color_vec|
+---+--------+------+--------------+-------------+-----------+-------------+
|  0|  apples|   red|           0.0|(4,[0],[1.0])|        1.0|(4,[1],[1.0])|
|  1| oranges|orange|           1.0|(4,[1],[1.0])|        0.0|(4,[0],[1.0])|
|  2|  banana|yellow|           2.0|(4,[2],[1.0])|        2.0|(4,[2],[1.0])|
|  3|  apples|   red|           0.0|(4,[0],[1.0])|        1.0|(4,[1],[1.0])|
|  4|  banana|yellow|           2.0|(4,[2],[1.0])|        2.0|(4,[2],[1.0])|
|  5| coconut| brown|           3.0|(4,[3],[1.0])|        3.0|(4,[3],[1.0])|
|  6| oranges|orange|           1.0|(4,[1],[1.0])|        0.0|(4,[0],[1.0])|
+---+--------+------+--------------+-------------+-----------+-------------+

// In above dataframe, the vector representation is Sparse-Vector representation.
// First component is the size of the vector
// Second component represents the indices where the vector is populated.
// Third component represents what values are in these indices.




// Converting Sparse-Vector to Dense-Vector
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors

val sparseToDense = udf((v: Vector) => v.toDense)

val df_denseVectors = df_transformed.withColumn("dense_category_vec", sparseToDense($"category_vec"))


scala> df_denseVectors.show()
+---+--------+------+--------------+-------------+-----------+-------------+------------------+
| id|category| color|category_Index| category_vec|color_Index|    color_vec|dense_category_vec|
+---+--------+------+--------------+-------------+-----------+-------------+------------------+
|  0|  apples|   red|           0.0|(4,[0],[1.0])|        1.0|(4,[1],[1.0])| [1.0,0.0,0.0,0.0]|
|  1| oranges|orange|           1.0|(4,[1],[1.0])|        0.0|(4,[0],[1.0])| [0.0,1.0,0.0,0.0]|
|  2|  banana|yellow|           2.0|(4,[2],[1.0])|        2.0|(4,[2],[1.0])| [0.0,0.0,1.0,0.0]|
|  3|  apples|   red|           0.0|(4,[0],[1.0])|        1.0|(4,[1],[1.0])| [1.0,0.0,0.0,0.0]|
|  4|  banana|yellow|           2.0|(4,[2],[1.0])|        2.0|(4,[2],[1.0])| [0.0,0.0,1.0,0.0]|
|  5| coconut| brown|           3.0|(4,[3],[1.0])|        3.0|(4,[3],[1.0])| [0.0,0.0,0.0,1.0]|
|  6| oranges|orange|           1.0|(4,[1],[1.0])|        0.0|(4,[0],[1.0])| [0.0,1.0,0.0,0.0]|
+---+--------+------+--------------+-------------+-----------+-------------+------------------+





_____________________________________________________________________________________________________________________________________________________

3. Vector Assembler

// Vector assembler’s job is to combine the raw features and features generated from various transforms into a single feature vector.
// It accepts boolean, numerical and vector type inputs.


import org.apache.spark.ml.feature.VectorAssembler


val encodedFeatures = features.flatMap({ name => 
	val stringIndexer = new StringIndexer().setInputCol(name).setOutputCol(name + "_Index")	
	val oneHotEncoder = new OneHotEncoderEstimator().setInputCols(Array(name + "_Index")).setOutputCols(Array(name + "_vec")).setDropLast(false)
	
	Array(stringIndexer, oneHotEncoder) // Return an array (although containing only one element)

})

val pipeline = new Pipeline().setStages(encodedFeatures)
val indexer_model = pipeline.fit(df)

val df_transformed = indexer_model.transform(df)

val vecFeatures = df_transformed.columns.filter(_.contains("vec")).toArray
// res29: Array[String] = Array(category_vec, color_vec)

val vectorAssembler = new VectorAssembler().setInputCols(vecFeatures).setOutputCol("features")

val pipelineVectorAssembler = new Pipeline().setStages(Array(vectorAssembler))

val result_df = pipelineVectorAssembler.fit(df_transformed).transform(df_transformed)


scala> result_df.show()
+---+--------+------+--------------+-------------+-----------+-------------+-------------------+
| id|category| color|category_Index| category_vec|color_Index|    color_vec|           features|
+---+--------+------+--------------+-------------+-----------+-------------+-------------------+
|  0|  apples|   red|           0.0|(4,[0],[1.0])|        1.0|(4,[1],[1.0])|(8,[0,5],[1.0,1.0])|
|  1| oranges|orange|           1.0|(4,[1],[1.0])|        0.0|(4,[0],[1.0])|(8,[1,4],[1.0,1.0])|
|  2|  banana|yellow|           2.0|(4,[2],[1.0])|        2.0|(4,[2],[1.0])|(8,[2,6],[1.0,1.0])|
|  3|  apples|   red|           0.0|(4,[0],[1.0])|        1.0|(4,[1],[1.0])|(8,[0,5],[1.0,1.0])|
|  4|  banana|yellow|           2.0|(4,[2],[1.0])|        2.0|(4,[2],[1.0])|(8,[2,6],[1.0,1.0])|
|  5| coconut| brown|           3.0|(4,[3],[1.0])|        3.0|(4,[3],[1.0])|(8,[3,7],[1.0,1.0])|
|  6| oranges|orange|           1.0|(4,[1],[1.0])|        0.0|(4,[0],[1.0])|(8,[1,4],[1.0,1.0])|
+---+--------+------+--------------+-------------+-----------+-------------+-------------------+


// Vector assembler, generally, will always feature in your workflow when you want to combine all your features before training or scoring your model


_____________________________________________________________________________________________________________________________________________________

4. Vector Indexer

// One of the transforms that also exist in spark is the vector indexer.
// Vector indexer in our example would let us skip the one hot encoding stage for encoding the categorical features.
// The algorithm infers, based on the values of the features, automatically and does the transformation to get the resulting feature vector. 


import org.apache.spark.ml.feature.VectorIndexer

// Just taking the string indexed features
val features = df_transformed.columns.filter(_.contains("Index")).toArray

//assembler
val VectorAssembler = new VectorAssembler().setInputCols(features).setOutputCol("features")

// vector indexer
val vectorIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexed_features").setMaxCategories(2)

// adding them to the pipeline
val pipeline = new Pipeline().setStages(Array(vectorAssembler, vectorIndexer))

val indexer_model = pipeline.fit(df_transformed)

// the indexer will infer and transformed accordinglt
val result_df = indexer_model.transform(df_transformed)


scala> result_df.show()
+---+--------+------+--------------+-------------+-----------+-------------+-------------------+-------------------+
| id|category| color|category_Index| category_vec|color_Index|    color_vec|           features|   indexed_features|
+---+--------+------+--------------+-------------+-----------+-------------+-------------------+-------------------+
|  0|  apples|   red|           0.0|(4,[0],[1.0])|        1.0|(4,[1],[1.0])|(8,[0,5],[1.0,1.0])|(8,[0,5],[1.0,1.0])|
|  1| oranges|orange|           1.0|(4,[1],[1.0])|        0.0|(4,[0],[1.0])|(8,[1,4],[1.0,1.0])|(8,[1,4],[1.0,1.0])|
|  2|  banana|yellow|           2.0|(4,[2],[1.0])|        2.0|(4,[2],[1.0])|(8,[2,6],[1.0,1.0])|(8,[2,6],[1.0,1.0])|
|  3|  apples|   red|           0.0|(4,[0],[1.0])|        1.0|(4,[1],[1.0])|(8,[0,5],[1.0,1.0])|(8,[0,5],[1.0,1.0])|
|  4|  banana|yellow|           2.0|(4,[2],[1.0])|        2.0|(4,[2],[1.0])|(8,[2,6],[1.0,1.0])|(8,[2,6],[1.0,1.0])|
|  5| coconut| brown|           3.0|(4,[3],[1.0])|        3.0|(4,[3],[1.0])|(8,[3,7],[1.0,1.0])|(8,[3,7],[1.0,1.0])|
|  6| oranges|orange|           1.0|(4,[1],[1.0])|        0.0|(4,[0],[1.0])|(8,[1,4],[1.0,1.0])|(8,[1,4],[1.0,1.0])|
+---+--------+------+--------------+-------------+-----------+-------------+-------------------+-------------------+




_____________________________________________________________________________________________________________________________________________________

[https://towardsdatascience.com/feature-encoding-made-simple-with-spark-2-3-0-part-2-5bfc869a809a]



val wine_data_csv = spark.read.format("csv").option("header", true).option("inferSchema", true).load("file:///D:/NotebookShare/Material/Hadoop/Spark/Datasets/RedWine/wineQualityReds.csv")


val wineData = wine_data_csv.
    withColumnRenamed("_c0", "ID").
    withColumnRenamed("fixed.acidity", "fixed_acidity").
    withColumnRenamed("volatile.acidity", "volatile_acidity").
    withColumnRenamed("citric.acid", "citric_acid").
    withColumnRenamed("residual.sugar", "residual_sugar").
    withColumnRenamed("free.sulfur.dioxide", "free_sulfur_dioxide").
    withColumnRenamed("total.sulfur.dioxide", "total_sulfur_dioxide")

// note: had to change the names here because spark had a problem with recognising "." as a column name.



// Spark gives us a number of options to do transformations to standardize/normalize the data. 
// I’ll go through the StandardScaler, Normalizer and the MinMaxScaler here. 
// To implement any of the aforementioned transformations, we need to assemble the features into a feature vector. 
// Any guesses on what we can use here? Correct, Vector Assemblers!


val wineFeatures = wineData.columns.filterNot(_.contains("ID")).filterNot(_.contains("quality"))

val wineVectorAssembler = new VectorAssembler().setInputCols(wineFeatures).setOutputCol("features")

val pipelineVectorAssembler = new Pipeline().setStages(Array(wineVectorAssembler))

val result_df = pipelineVectorAssembler.fit(wineData).transform(wineData)



scala> result_df.show(10)
2019-04-21 14:45:49 WARN  CSVDataSource:66 - CSV header does not conform to the schema.
 Header: , fixed.acidity, volatile.acidity, citric.acid, residual.sugar, chlorides, free.sulfur.dioxide, total.sulfur.dioxide, density, pH, sulphates, alcohol, quality
 Schema: _c0, fixed.acidity, volatile.acidity, citric.acid, residual.sugar, chlorides, free.sulfur.dioxide, total.sulfur.dioxide, density, pH, sulphates, alcohol, quality
Expected: _c0 but found:
CSV file: file:///D:/NotebookShare/Material/Hadoop/Spark/Datasets/RedWine/wineQualityReds.csv
+---+-------------+----------------+-----------+--------------+---------+-------------------+--------------------+-------+----+---------+-------+-------+--------------------+
| ID|fixed_acidity|volatile_acidity|citric_acid|residual_sugar|chlorides|free_sulfur_dioxide|total_sulfur_dioxide|density|  pH|sulphates|alcohol|quality|            features|
+---+-------------+----------------+-----------+--------------+---------+-------------------+--------------------+-------+----+---------+-------+-------+--------------------+
|  1|          7.4|             0.7|        0.0|           1.9|    0.076|               11.0|                34.0| 0.9978|3.51|     0.56|    9.4|      5|[7.4,0.7,0.0,1.9,  ]|
|  2|          7.8|            0.88|        0.0|           2.6|    0.098|               25.0|                67.0| 0.9968| 3.2|     0.68|    9.8|      5|[7.8,0.88,0.0,2.6..]|
|  3|          7.8|            0.76|       0.04|           2.3|    0.092|               15.0|                54.0|  0.997|3.26|     0.65|    9.8|      5|[7.8,0.76,0.04,2...]|
|  4|         11.2|            0.28|       0.56|           1.9|    0.075|               17.0|                60.0|  0.998|3.16|     0.58|    9.8|      6|[11.2,0.28,0.56,1..]|
|  5|          7.4|             0.7|        0.0|           1.9|    0.076|               11.0|                34.0| 0.9978|3.51|     0.56|    9.4|      5|[7.4,0.7,0.0,1.9,..]|
|  6|          7.4|            0.66|        0.0|           1.8|    0.075|               13.0|                40.0| 0.9978|3.51|     0.56|    9.4|      5|[7.4,0.66,0.0,1.8..]|
|  7|          7.9|             0.6|       0.06|           1.6|    0.069|               15.0|                59.0| 0.9964| 3.3|     0.46|    9.4|      5|[7.9,0.6,0.06,1.6..]|
|  8|          7.3|            0.65|        0.0|           1.2|    0.065|               15.0|                21.0| 0.9946|3.39|     0.47|   10.0|      7|[7.3,0.65,0.0,1.2..]|
|  9|          7.8|            0.58|       0.02|           2.0|    0.073|                9.0|                18.0| 0.9968|3.36|     0.57|    9.5|      7|[7.8,0.58,0.02,2...]|
| 10|          7.5|             0.5|       0.36|           6.1|    0.071|               17.0|               102.0| 0.9978|3.35|      0.8|   10.5|      5|[7.5,0.5,0.36,6.1..]|
+---+-------------+----------------+-----------+--------------+---------+-------------------+--------------------+-------+----+---------+-------+-------+--------------------+

// I think we got the above WARNIGN because we explicitly changes the schema of the csv file.



_____________________________________________________________________________________________________________________________________________________


StandardScaler

// The format for applying this transformation is very similar to what we saw while encoding the categorical features. 
// It transforms a dataset of Vector rows, normalizing each feature to have unit standard deviation and zero mean.

// However, there is a caveat with standardization in spark. 
// Unfortunately, standard scaler does not internally convert the sparse vector to a dense vector. 
// Hence it is very important to convert the sparse vector to a dense vector before running this step

// While our dataframe doesn’t really have sparse vectors, spark can sometimes change the output of the vector assembler to sparse vectors and this 
// can give some incorrect results as this doesn’t really throw an error.


import org.apache.spark.ml.feature.StandardScaler

// convert the sparse vector to a dense vector as a fail safe

val sparseToDense = udf((v : Vector) => v.toDense)

val result_df_dense = result_df.withColumn("features", sparseToDense($"features"))

val scaler = new StandardScaler().
	setInputCol("features").
	setOutputCol("scaledFeatures").
  	setWithStd(true).
  	setWithMean(true)

// Compute summary statistics by fitting the StandardScaler.
val scalerModel = scaler.fit(result_df_dense)

// Normalize each feature to have unit standard deviation.
val scaledData = scalerModel.transform(result_df_dense)

scaledData.select("features", "scaledFeatures")).show()



scala> scaledData.select("features", "scaledFeatures").show(10)
+--------------------+--------------------+
|            features|      scaledFeatures|
+--------------------+--------------------+
|[7.4,0.7,0.0,1.9,..]|[-0.5281943702487..]|
|[7.8,0.88,0.0,2.6..]|[-0.2984540647668..]|
|[7.8,0.76,0.04,2...]|[-0.2984540647668..]|
|[11.2,0.28,0.56,1..]|[1.65433853182897..]|
|[7.4,0.7,0.0,1.9,..]|[-0.5281943702487..]|
|[7.4,0.66,0.0,1.8..]|[-0.5281943702487..]|
|[7.9,0.6,0.06,1.6..]|[-0.2410189883963..]|
|[7.3,0.65,0.0,1.2..]|[-0.5856294466191..]|
|[7.8,0.58,0.02,2...]|[-0.2984540647668..]|
|[7.5,0.5,0.36,6.1..]|[-0.4707592938782..]|
+--------------------+--------------------+


scala> scaledData.select("scaledFeatures").show(10, truncate=false)
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|scaledFeatures                                                                                                                                                                                                                |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|[-0.528194370248706,0.9615758497615392,-1.3910371025826445,-0.45307666524694107,-0.24363046870568492,-0.4660467180570466,-0.37901411729367135,0.5580998653529516,1.288239900932479,-0.5790253785668811,-0.959945795360617]    |
|[-0.2984540647668453,1.9668271476507533,-1.3910371025826445,0.04340256663998611,0.22380518049685372,0.8723653192008117,0.624167961720931,0.028251929345686128,-0.7197081416221638,0.12891007217488534,-0.5845942253085579]    |
|[-0.2984540647668453,1.2966596157246109,-1.185699492480436,-0.16937424702583995,0.0963227307143431,-0.08364327884051564,0.22897502150305737,0.13422151654712747,-0.3310730366115898,-0.04807379051055643,-0.5845942253085579] |
|[1.6543385318289732,-1.3840105119799593,1.4836894388482746,-0.45307666524694107,-0.26487754366943667,0.10755844076774984,0.41137176314207596,0.664069452554393,-0.978798211629215,-0.4610361367765872,-0.5845942253085579]    |
|[-0.528194370248706,0.9615758497615392,-1.3910371025826445,-0.45307666524694107,-0.24363046870568492,-0.4660467180570466,-0.37901411729367135,0.5580998653529516,1.288239900932479,-0.5790253785668811,-0.959945795360617]    |
|[-0.528194370248706,0.7381866724528254,-1.3910371025826445,-0.5240022698022162,-0.26487754366943667,-0.27484499844878113,-0.19661737565465273,0.5580998653529516,1.288239900932479,-0.5790253785668811,-0.959945795360617]    |
|[-0.24101898839637972,0.40310290648975383,-1.0830306874293318,-0.6658534789127668,-0.392359993451947,-0.08364327884051564,0.38097230620223954,-0.18368724505725537,-0.0719829666045386,-1.1689715875183533,-0.959945795360617]|
|[-0.5856294466191716,0.6823393781256468,-1.3910371025826445,-0.949555897133868,-0.47734829330695405,-0.08364327884051564,-0.7742070575115451,-1.1374135298702863,0.510969690911328,-1.1099769666232064,-0.39691844028252915]  |
|[-0.2984540647668453,0.29140831783539667,-1.2883682975315403,-0.3821510606916657,-0.3073716935969402,-0.6572484376653122,-0.8654054283310544,0.028251929345686128,0.31665213840603823,-0.5200307576717345,-0.8661079028476025]|
|[-0.47075929387824095,-0.15537003678203148,0.45700138833723186,2.525798726074621,-0.3498658435244438,0.10755844076774984,1.6881489546152062,0.5580998653529516,0.25187962090427685,0.8368455229166518,0.0722710222825443]     |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+



_____________________________________________________________________________________________________________________________________________________


Normalizer

// Normalizer normalizes each vector to have unit norm. 
// It accepts a parameter p(default=2) which specifies the p-norm used for normalization. 
// This is generally useful when you have sparse datasets with features on different scales. 

// Don’t forget to use dense vectors as an input!


import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors

// Normalize each Vector using $L^1$ norm.
val normalizer = new Normalizer().
	setInputCol("features").
	setOutputCol("normFeatures").
	setP(1.0)

val l1NormData = normalizer.transform(result_df_dense)

l1NormData.select($"features", $"normFeatures").show(10, truncate = false)

scala> l1NormData.select($"normFeatures").show(10, truncate = false)
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|normFeatures                                                                                                                                                                                                                       |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|[0.10640776028919903,0.010065598946275582,0.0,0.027320911425605155,0.0010928364570242063,0.15817369772718776,0.4889005202476712,0.014347792326562539,0.050471789002039,0.008052479157020467,0.13516661442141498]                   |
|[0.0660710110897651,0.007454165353717087,0.0,0.022023670363255032,8.301229598457665E-4,0.21176606118514452,0.5675330439761873,0.008443536391574082,0.027106055831698502,0.005760036864235932,0.08301229598457666]                  |
|[0.08236623406794158,0.008025427934825078,4.22390943938162E-4,0.024287479276444313,9.714991710577726E-4,0.15839660397681074,0.5702277743165187,0.010528094277658688,0.0344248619309602,0.006863852838995132,0.1034857812648497]    |
|[0.10610783208435572,0.0026526958021088937,0.005305391604217787,0.018000435800024632,7.105435184220249E-4,0.16105653084232566,0.56843481473762,0.009454965751802413,0.029937566909514653,0.0054948698757969925,0.09284435307381127]|
|[0.10640776028919903,0.010065598946275582,0.0,0.027320911425605155,0.0010928364570242063,0.15817369772718776,0.4889005202476712,0.014347792326562539,0.050471789002039,0.008052479157020467,0.13516661442141498]                   |
|[0.09560377660756457,0.008526823319053056,0.0,0.023254972688326516,9.689571953469382E-4,0.16795258052680262,0.5167771708517004,0.012891006526895666,0.0453471967422367,0.007234880391923806,0.1214426351501496]                    |
|[0.08029646675218072,0.006098465829279548,6.098465829279548E-4,0.016262575544745462,7.01323570367148E-4,0.1524616457319887,0.5996824732124889,0.010127518920490236,0.03354156206103751,0.00467549046911432,0.09554263132537959]    |
|[0.12152569685831102,0.010820781227109886,0.0,0.01997682688081825,0.0010820781227109885,0.24971033601022813,0.3495944704143194,0.016557460013051528,0.05643453593831156,0.007824257194987147,0.1664735573401521]                   |
|[0.15028959649170132,0.011175380251947021,3.853579397223111E-4,0.03853579397223111,0.0014065564799864354,0.17341107287503998,0.34682214575007997,0.019206239715759986,0.06474013387334826,0.010982701282085864,0.18304502136809775]|
|[0.050275240181580756,0.00335168267877205,0.002413211528715876,0.04089052868101901,4.759389403856311E-4,0.1139572110782497,0.6837432664694982,0.0066886179537575035,0.022456273947772738,0.00536269228603528,0.07038533625421306]  |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+




_____________________________________________________________________________________________________________________________________________________


MinMaxScaler

// Transforms features by scaling each feature to a given range. 
// This is generally used when the distribution followed by the data is not Gaussian, as assumed by the Standard Scaler.


import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors

val scaler = new MinMaxScaler().
	setInputCol("features").
	setOutputCol("minMaxScaledFeatures")

// Compute summary statistics and generate MinMaxScalerModel
val minMaxScalerModel = scaler.fit(result_df_dense)

// rescale each feature to range [min, max].
val minMaxScaledData = minMaxScalerModel.transform(result_df_dense)

minMaxScaledData.select("minMaxScaledFeatures").show(10, truncate = false)


scala> minMaxScaledData.select("minMaxScaledFeatures").show(10, truncate = false)
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|minMaxScaledFeatures                                                                                                                                                                                       |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|[0.24778761061946908,0.3972602739726027,0.0,0.0684931506849315,0.10684474123539232,0.14084507042253522,0.0989399293286219,0.5675477239353917,0.6062992125984251,0.13772455089820362,0.15384615384615385]   |
|[0.2831858407079646,0.5205479452054794,0.0,0.11643835616438358,0.14357262103505844,0.3380281690140845,0.21554770318021202,0.49412628487518584,0.3622047244094489,0.20958083832335334,0.21538461538461545]  |
|[0.2831858407079646,0.4383561643835617,0.04,0.0958904109589041,0.13355592654424042,0.19718309859154928,0.1696113074204947,0.5088105726872254,0.4094488188976376,0.19161676646706588,0.21538461538461545]   |
|[0.5840707964601769,0.10958904109589043,0.56,0.0684931506849315,0.10517529215358933,0.22535211267605634,0.19081272084805653,0.5822320117474312,0.3307086614173229,0.1497005988023952,0.21538461538461545]  |
|[0.24778761061946908,0.3972602739726027,0.0,0.0684931506849315,0.10684474123539232,0.14084507042253522,0.0989399293286219,0.5675477239353917,0.6062992125984251,0.13772455089820362,0.15384615384615385]   |
|[0.24778761061946908,0.36986301369863017,0.0,0.06164383561643836,0.10517529215358933,0.16901408450704225,0.12014134275618374,0.5675477239353917,0.6062992125984251,0.13772455089820362,0.15384615384615385]|
|[0.29203539823008856,0.3287671232876712,0.06,0.04794520547945206,0.09515859766277131,0.19718309859154928,0.1872791519434629,0.4647577092510986,0.4409448818897636,0.07784431137724551,0.15384615384615385] |
|[0.23893805309734514,0.363013698630137,0.0,0.020547945205479447,0.08848080133555927,0.19718309859154928,0.053003533568904596,0.33259911894273464,0.5118110236220473,0.0838323353293413,0.2461538461538461] |
|[0.2831858407079646,0.3150684931506849,0.02,0.07534246575342467,0.1018363939899833,0.11267605633802817,0.04240282685512368,0.49412628487518584,0.48818897637795267,0.14371257485029937,0.16923076923076918]|
|[0.25663716814159293,0.2602739726027397,0.36,0.3561643835616438,0.09849749582637729,0.22535211267605634,0.3392226148409894,0.5675477239353917,0.48031496062992135,0.28143712574850305,0.32307692307692304] |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+


_____________________________________________________________________________________________________________________________________________________


// The above three are generally the most used feature scaling methods. 
// I want to go through some other spark transformations that we can do when we have to deal with continuous features.


Bucketizer

// The bucketizer transforms a column of continuous features to a column of feature buckets. The buckets are decided by the parameter "splits". 
// A bucket defined by the splits x, y holds values in the range [x, y) except the last bucket which also includes y. 
// The splits here should be strictly increasing. 
// If the upper limits/lower limits are unknown then infinity should be provided as a the min and max to avoid any out of Bucketizer bounds exception. 
// This does not handle NAN values so its good practice to get rid of those before.


import org.apache.spark.ml.feature.Bucketizer

// Define the splits that I want
val splits = Array(0.0, 0.1, 0.2, 0.3, 0.5, 1.0) 

val bucketizer = new Bucketizer().
	setInputCol("citric_acid").
	setOutputCol("bucketedFeatures").
	setSplits(splits)

// Transform original data into its bucket index.
val bucketedData = bucketizer.transform(wineData)

scala> bucketedData.select($"citric_acid", $"bucketedFeatures").show(10, truncate=false)
+-----------+----------------+
|citric_acid|bucketedFeatures|
+-----------+----------------+
|0.0        |0.0             |
|0.0        |0.0             |
|0.04       |0.0             |
|0.56       |4.0             |
|0.0        |0.0             |
|0.0        |0.0             |
|0.06       |0.0             |
|0.0        |0.0             |
|0.02       |0.0             |
|0.36       |3.0             |
+-----------+----------------+



_____________________________________________________________________________________________________________________________________________________


Quantile Discretizer

// This is very similar to the bucketizer and useful when you don’t really have defined splits. 
// It can also handle NANs. 
// There is a "handleInvalid" setter to choose to remove the NAN or raise an exception, when encountered. 
// If the NANs are set then a special bucket will be created to handle those.

import org.apache.spark.ml.feature.QuantileDiscretizer

val discretizer = new QuantileDiscretizer().
	setInputCol("citric_acid").
	setOutputCol("quantileBins").
	setNumBuckets(10).
	setHandleInvalid("skip")

val result = discretizer.fit(wineData).transform(wineData)

scala> result.select($"citric_acid", $"quantileBins").show(10)
+-----------+------------+
|citric_acid|quantileBins|
+-----------+------------+
|        0.0|         0.0|
|        0.0|         0.0|
|       0.04|         1.0|
|       0.56|         9.0|
|        0.0|         0.0|
|        0.0|         0.0|
|       0.06|         1.0|
|        0.0|         0.0|
|       0.02|         1.0|
|       0.36|         6.0|
+-----------+------------+


_____________________________________________________________________________________________________________________________________________________

