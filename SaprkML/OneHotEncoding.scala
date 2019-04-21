[https://community.hortonworks.com/articles/75295/spark-categorical-data-transformation.html]


Index categorial features - StringIndexer
Encode to one hot vectors - OneHotEncoder
Assemble to a feature vector


_____________________________________________________________________________________________________________________________________________________


import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

val df = spark.createDataFrame(Seq(
    (0, "apple"),
    (1, "banana"),
    (2, "coconut"),
    (1, "banana"),
    (2, "coconut")
    )
).toDF("id", "fruit")


_____________________________________________________________________________________________________________________________________________________


// StringIndexer - Index categorial features

// The StringIndex function maps a string column of labels to an ML column of label indices. 
// In our example, this code will traverse through the dataframe and create a matching index for each of the values in the fruit name column.

// String Indexer encodes a column of string labels/categories to a column of indices. 
// The ordering of the indices is done on the basis of popularity and the range is [0, numOfLabels).

import org.apache.spark.ml.feature.StringIndexer

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

_____________________________________________________________________________________________________________________________________________________


//  OneHotEncoder - Encode to one hot vectors

// One hot encoder maps the label indices to a binary vector representation with at the most a single one-value. 
// These methods are generally used when we need to use categorical features but the algorithm expects continuous features.

// The spark one hot encoder takes the indexed label/category from the string indexer and then encodes it into a sparse vector. 
// This is slightly different from the usual dummy column creation style. 


// The OneHotEncoder function maps a column of category indices to a column of binary vectors. 
// In our example, this code will convert the values into a binary vector and ensure only one of them is set to true or hot.

import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.OneHotEncoderEstimator

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




_____________________________________________________________________________________________________________________________________________________


