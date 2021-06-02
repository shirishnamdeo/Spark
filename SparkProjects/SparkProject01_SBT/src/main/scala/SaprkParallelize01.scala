import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.log4j.Logger
import org.apache.log4j.Level


object SaprkParallelize01 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkParallelize01")

    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    // There is one more method to set the logging level with spark context also.

    val scala_data = Array(1,2,3,4,5,6)
    println(scala_data)
    // [I@2a415aa9 -- The content of the object is not printed

    println(scala_data.length)
    // 6

    scala_data.map(x => print(x))
    // 123456

    // Note that we are using sc to make an RDD now.
    val parallelize_rdd = sc.parallelize(scala_data, 3)
    // slice is a synonym of partition (backward compatibility)

    println(parallelize_rdd.partitions.size)
    // 3

    println(parallelize_rdd.count())
    // 6

    val reduced_rdd = parallelize_rdd.reduce((a, b) => a + b)
    println(reduced_rdd)
    // 21
    // Why the return value is not an RDD, and just a Numeric type?
    // See how reduce works!!



  }
}
