import org.apache.spark.{SparkConf, SparkContext}

import org.apache.log4j.{Logger, Level}

object SparkParallelize01 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkParallelize01")

    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val parallelize_rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks")))

    println(parallelize_rdd)  // ParallelCollectionRDD[0]
    println(parallelize_rdd.count)  // 2
    println(parallelize_rdd.partitions.size)  // 8 Strainge!! Why 8 partitions


    val parallelize_rdd2 = sc.parallelize(Seq((1, "Spark"), (2, "Databricks")), 3)
    println(parallelize_rdd)  // ParallelCollectionRDD[0]
    println(parallelize_rdd.count)  // 2
    println(parallelize_rdd.partitions.size)  // 8 Strange!! Again 8 partitions
    // Slices vs Partitions

    // To DataSet
    // println(parallelize_rdd.toDS()) -- Not found!!!


  }
}
