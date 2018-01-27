package kafka_stream


import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import java.sql.Timestamp

import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import java.util._
//import java.util
//import java.util._

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Durations

import org.apache.kafka.common.TopicPartition


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}





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




object kafka_stream extends InitSpark {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)



    //val dataframe = spark.read.option("mergeSchema", "true").csv("hdfs://localhost:8020/tmp/data/")
     // .selectExpr("CAST(value AS STRING)")
     // .as[(String)]

   // val test  =spark.read.option("mergeSchema", "true")
   //   .csv("hdfs://localhost:8020/tmp/data/")
   //   .as[(String)]
   //   .map(_.split(",").map(_.trim))

  //  import spark.implicits._

  // airports
    /*val test: Dataset[MyRow] = spark.read
      .option("header", "false").option("delimiter", ",")
      .csv("hdfs://localhost:8020/tmp/data/").as[String]
      .map(_.split(",").map(_.trim)).map(
      line => MyRow(line(0).toString, line(1).toString, line(2).toString, line(3).toString,line(4).toString,
        line(5).toString,line(6).toString)
    ) */

    // carriers



    //val header = test.first
    // filter out header (eh. just check if the first val matches the first header name)
    //val data = test.filter(_(0) != header(0)).toDF()

    //test.show()

    //test.createOrReplaceTempView("test")
    //spark.sql("SELECT * FROM test").show()

    read_from_kafka()

  //  from_decomposed_modelled()

  //  exploration()

    DirectKafkaWordCount()

  }

  def from_decomposed_modelled():Unit = {

    spark.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension", "false")


    val df_airports = spark.read
      .format("com.databricks.spark.avro")
      .load("hdfs://localhost:8020/data/decomposed/airports/").as[airports]

    val df_planedate = spark.read
      .format("com.databricks.spark.avro")
      .load("hdfs://localhost:8020/data/decomposed/planedate/").as[planedate]


    //df_airports.select("country").distinct().show(100)
    //df_planedate.select("origin").distinct().show(100)

    // remove duplicates
    //df.distinct()
    //df.dropDuplicates().show()
    //df.dropDuplicates(Array("iata", "state")).show()


    //df_planedate.join(df_airports,Seq("user_id"),joinType="outer")

    //df_planedate.join(df_airports, $"origin" === $"country", joinType="inner").show(5)



    // option #1 - Spark Dataframe
    df_planedate.write.mode(SaveMode.Overwrite)
                .parquet("hdfs://localhost:8020/data/modelled/planedate/")

    df_airports.write.mode(SaveMode.Overwrite)
                .parquet("hdfs://localhost:8020/data/modelled/airports/")


   // val df_planedate_modelled = spark.read
   //                                   .parquet("hdfs://localhost:8020/data/modelled/planedate/")
   //                                   .as[planedate]

  //  val df_airports_modelled = spark.read
  //                                  .parquet("hdfs://localhost:8020/data/modelled/airports/")
  //                                  .as[airports]



    //df_airports_modelled.show(5)

    //df_planedate_modelled.createOrReplaceTempView("temp_table")

    //spark.sql("SELECT * FROM temp_table where carrierdelay <>'NA'").show(5)

    // option #2 - Spark SQL
    //spark.sql("INSERT OVERWRITE TABLE modelled.planedate select * from temp_table")



  }

  //Scala example for direct approach
  // building data streaimg application apache kafka

  def  DirectKafkaWordCount ():Unit = {
    val brokers = "localhost:9092"
    val topics = "test"

   // val sparkConf = new SparkConf().setMaster("local").setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(spark.sparkContext, Durations.seconds(2))

    val topicsSet = topics.split(",").toSet


    //val kafkaParams = Map[String,String] ("metadata.broker.list" -> brokers)

    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)

   /* val kafkaParams1 = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
*/
    /*val topics1 = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics1, kafkaParams1)
    )
    val lines = stream.map(record => (record.key, record.value)).map(_._2)

    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.map(x=>(x,1)).reduceByKey(_+_)
    //wordCounts.dstream().saveAsTextFiles("hdfs://10.200.99.197:8020/user/chanchal.singh/wordCounts", "result");
    wordCounts.print()*/




    val messages = KafkaUtils
      .createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams, topicsSet)


    val lines = messages.map(_._2)

    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.map(x=>(x,1)).reduceByKey(_+_)
    //wordCounts.dstream().saveAsTextFiles("hdfs://10.200.99.197:8020/user/chanchal.singh/wordCounts", "result");
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

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

   def read_from_kafka():Unit ={


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
    //.as[(String)].map(_.split(",").map(_.trim))




      //   val result = df.
 //     select(
 //       $"key" cast "string",   // deserialize keys
 //       $"value" cast "string") // deserialize values
    //    $"topic",
    //    $"partition",
    //    $"offset")
 //      .toDF("key", "value")



    // Split the lines into words
    //val words = lines.as[String].flatMap(_.split(” “))
    // Generate running word count
    //val wordCounts = words.groupBy(“value”).count()


    //df.write.parquet("hdfs://localhost:19000/tmp/test")

    val query = values //.orderBy("window")
      .repartition(1)
      .writeStream
      .outputMode(OutputMode.Append())
      .format("csv")
      .option("checkpointLocation", "hdfs://localhost:8020/tmp/checkpoints")
      .option("path", "hdfs://localhost:8020/data/raw/carriers")
      .start()
      .awaitTermination()

    //query.stop()


    //val usersDF = spark.read.load("hdfs://localhost:8020/tmp/data/")
    //usersDF.createOrReplaceTempView("test")
    //spark.sql("SELECT * FROM test").show()

   //spark.sql("SELECT * FROM parquet.`hdfs://localhost:8020/tmp/data/`").show()



   // query.stop()

   /* values.writeStream
      .trigger(ProcessingTime("5 seconds"))
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()*/

    /*writeStream
      .format("parquet")
      .option("path", "path/to/destination/dir")
      .start()*/
  }


}
