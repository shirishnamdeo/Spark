import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SparkSequenceFile01 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkSequenceFile01")

    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val data_rdd = sc.textFile("file:///D:/NotebookShare/Material/Hadoop/DataSet/violations_plus.csv")

    data_rdd.map(x => println(x))
    // Why print is not working with MAP
    // Because MAP is a transformation, which generate a new RDD and not an action (return a value to driver program)

    // data_rdd.map(x => (NullWritable.get(), x).saveAsSequenceFile("file:///D:/NotebookShare/Material/Hadoop/Spark/IntellijSparkProjects/SparkProject_Maven/target/data_output/")
    // saveAsSequenceFile is not recognized

  }
}
