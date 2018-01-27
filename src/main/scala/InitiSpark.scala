package kafka_stream

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

trait InitSpark {
  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming Example")
    //.master("local[*]")
    .master("spark://127.0.01:7077")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  def reader = spark.read
    .option("header",true)
    .option("inferSchema", false)
    .option("mode", "DROPMALFORMED")
    .option("dateFormat", "dd/MM/yyyy")

  def readerWithoutHeader = spark.read
    .option("header",true)
    .option("inferSchema", false)
    .option("mode", "DROPMALFORMED")
    .option("dateFormat", "MM/dd/yyyy")



  def close = {
    spark.close()
  }
}