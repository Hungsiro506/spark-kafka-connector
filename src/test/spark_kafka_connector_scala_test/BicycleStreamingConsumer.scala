package spark_kafka_connector_scala_test


import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Properties, UUID}

import com.google.gson.Gson
import com.mongodb.spark.MongoSpark
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession

//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.mongodb.spark.sql._
/**
  * Created by hungdv on 02/03/2017.
  */
object BicycleStreamingConsumer {

  private val appName= "Box usage analytic"
  private val master ="local"

  case class BikeAggreagation(bikeName: String, total: Int, dateTime:String)
  var brokers = ""

  def main(args: Array[String]): Unit ={
    val sparkSession = SparkSession.builder()
      .master(master).appName(appName)
      .config(SparkCommon.conf).getOrCreate()

    //get Kafka broker node
    brokers = scala.util.Try(args(0)).getOrElse("localhost:9092")

    //get the topic for consumer
    val topic = scala.util.Try(args(1)).getOrElse("bike-data")
    //get duration time for micro batch
    val batchDuration = scala.util.Try(args(2)).getOrElse("30").toInt //in second

    /**
      * We have some constructor of an StreamingContext :
      * StreamingContext( conf: SparkConfig, batchDuration: Duration)
      * StreamingContext( sparkContext : SparkContext, batchDuration: Duration)
      *
      * tom lai co the tao StreamingContext tu mot trong 2 : SparkConf hoac SparkContext
      * val sparkContext = SparkContext.getOrCreate(conf: SparkConfig) <- moi con duong deu dan toi thanh ROME !
      */

    // OLD API
    // METHOD 1 : CREATE STREAMING CONTEXT FIRST THEN GET SPARK CONTEXT FROM STREAMING
    //create Streaming Context from Spark
 /*   val streamContext = new StreamingContext(SparkCommon.conf,Seconds(batchDuration))
    val sparkContext =  streamContext.sparkContext*/

    // METHOD 2 : CREATE SPARKCONTEXT FIST THEN USING IT CO CONCREATE STREAMING CONTEXT
/*    val sparkContext = SparkContext.getOrCreate(SparkCommon.conf)
    val streamContext = new StreamingContext(sparkContext,Seconds(30))*/

    //METHOD 3: Newest Spark API, SparkSession rule them all.
    val sparkContext = sparkSession.sparkContext
    val streamingContext = new StreamingContext(sparkContext,Seconds(1))

    //create SQL context for using MongoDB
    val sqlContext = sparkSession.sqlContext
    // OLD API : import  sqlContext.implicits._ de co the convert DataFrame DataSet va RDD
    // NEW API : import sparkSession.implicits._
    import sparkSession.implicits._

    // Co the truyen vao mot set topic
    var outTopicsSet = Set(topic)

    var kafkaParams = Map[String,Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Maybe This is OLD API
    // Tra ve 1 RDD thoi.
  /*  var msg = KafkaUtils.createDirectStream[String,String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](
      streamingContext
      kafkaParams,
      outTopicsSet
    )*/

    // New API @since 2017-3
    // Tra ve mot tap RDD
    val msgs = KafkaUtils.createDirectStream[String,String](
      streamingContext,
      PreferConsistent,
      Subscribe[String,String](outTopicsSet,kafkaParams)
    )
    //get the value
    val value = msgs.map(record => record.value())
    val bicycles = value.map(x => x.split(",")(1))

    val bicycleDStream  = bicycles.map(x => (x,1))
    val aggregateBicycles = bicycleDStream.reduceByKey((x,y) => x+y)

    aggregateBicycles.print()


    val writeConfig = new util.HashMap[String,String]();

    aggregateBicycles.foreachRDD(
      rdd => {
        val today = Calendar.getInstance.getTime
        val formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        val data = rdd.map(x => BikeAggreagation(x._1,x._2,formater.format(today))).toDF()
        //OLD API:




        //NEW API Streaming context write data direcly to mongo
        //data.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://example.com/database.collection")))
        data.write.mode("append").mongo()
      }
    )

    /*
    Process received data here
    */

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}


