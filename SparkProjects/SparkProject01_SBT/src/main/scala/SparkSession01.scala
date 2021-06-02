// Strange!! Spark session is part of sql package.
import org.apache.spark.sql.SparkSession


object SparkSession01 {
  def main(args: Array[String]): Unit = {

    // Note the new keyword is not required, but while setting up the sparkConf and sparkContext, new is required.
    val spark = SparkSession
      .builder()
      .appName("SparkSession01")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // .appName -> Name of the application to be shown in the spark web UI
    // .enableHiveSupport -> To enable Hive Support, create spark session with Hive Support
    // .config("spark.some.config.option", "some-value") -> [key, value] pair(s)

    println(spark.version)

  }
}
