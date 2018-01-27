package airflights_streaming


import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, LogManager, Logger}

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}

case class airports(iata:String,	airport:String,	city:String,	state:String,	country:String,
                    lat:String,	longt:String, insert_time:String, uuid:String)

case class carriers(code:String,	description:String, insert_time:String, uuid:String )

case class planedate(Year: String, Month : String, DayofMonth : String, DayOfWeek : String, DepTime : String,
                     CRSDepTime : String, ArrTime : String, CRSArrTime : String, UniqueCarrier : String,
                     FlightNum : String, TailNum : String, ActualElapsedTime : String, CRSElapsedTime : String,
                     AirTime : String, ArrDelay : String, DepDelay : String, Origin : String, Dest : String,
                     Distance : String, TaxiIn : String, TaxiOut : String, Cancelled : String, CancellationCode : String,
                     Diverted : String, CarrierDelay : String, WeatherDelay : String, NASDelay : String,
                     SecurityDelay : String, LateAircraftDelay : String, insert_time: String, uuid : String )


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


object kafka_stream extends InitSpark {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // READ FROM KAFKA TOPIC AND INGEST INTO HDFS
    read_from_kafka()

    // USE SPARK SQL TO EXECUTE ETL PROCESS and EXPLORATION

    exploration()


  }

  def read_from_kafka():Unit = {


    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "carriers")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val values = df.selectExpr("CAST(value AS STRING)").as[String]

    val query = values //.orderBy("window")
      .repartition(1)
      .writeStream
      .outputMode(OutputMode.Append())
      .format("csv")
      .option("checkpointLocation", "hdfs://localhost:8020/tmp/checkpoints")
      .option("path", "hdfs://localhost:8020/data/raw/carriers")
      .start()
      .awaitTermination()

  }

  def  exploration ():Unit = {

    import org.apache.spark.sql.functions._

    val df_planedate_modelled = spark.read
      .parquet("hdfs://localhost:8020/data/modelled/planedate/")
      .as[planedate]

    val df_airports_modelled = spark.read
      .parquet("hdfs://localhost:8020/data/modelled/airports/")
      .as[airports]


    df_planedate_modelled.registerTempTable("airlineDF")

    //What are the primary causes for flight delays
    spark.sql("SELECT sum(WeatherDelay) Weather,sum(NASDelay) NAS,sum(SecurityDelay) Security, " +
      "sum(LateAircraftDelay) lateAircraft, sum(CarrierDelay) Carrier FROM airlineDF ").show()

    // Which Airports have the Most Delays

    spark.sql("SELECT Origin, count(*) conFlight,avg(DepDelay) delay FROM airlineDF GROUP BY Origin")
      .sort(desc("delay")).show()

    // Join airports and flights data
    df_planedate_modelled.join(df_airports_modelled, $"origin" === $"iata", joinType="inner").show(5)


    df_planedate_modelled.write.parquet("hdfs://localhost:8020/data/modelled/planedate/")

  }

}

