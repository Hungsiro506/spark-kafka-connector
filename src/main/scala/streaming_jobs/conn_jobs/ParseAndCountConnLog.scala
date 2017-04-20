package streaming_jobs.conn_jobs

import java.text.SimpleDateFormat
import java.util.Calendar

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.{Accumulable, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import parser.{AbtractLogLine, ConnLogLineObject, ConnLogParser}
import com.datastax.spark.connector.streaming._
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.sql._
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex
import streaming_jobs.conn_jobs.ConcurrentHashMapAccumulator

/**
  * Created by hungdv on 20/03/2017.
  */
object ParseAndCountConnLog {
  type WordCount = (String,Int)
  def parseAndCount(ssc: StreamingContext,
                    ss:SparkSession,
                    lines: DStream[String],
                    windowDuration: FiniteDuration,
                    slideDuration: FiniteDuration,
                    cassandraConfig: Map[String,Object],
                    mongoDbConfig: Map[String,Object],
                    conLogParser: ConnLogParser
                   ): Unit ={

    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): org.apache.spark.streaming.Duration =
      new Duration(value.toMillis)
    import ss.implicits._

    val sc = ssc.sparkContext
    val mc = new MapAccumulator()

    val bWindowDuration = sc.broadcast(windowDuration)
    val bSlideDuration  = sc.broadcast(slideDuration)

    val bConLogParser   = sc.broadcast(conLogParser)

    val objectConnLogs: DStream[ConnLogLineObject] = lines.transform(extractValue(bConLogParser))
        objectConnLogs.saveToCassandra(cassandraConfig("keySpace").toString,
                                       cassandraConfig("table").toString,
                                       SomeColumns("time","session_id","connect_type","name","content1","content2"))


    //Sorry, it was 7PM, i was too lazy to code. so i did too much hard code here :)).
    val connType = objectConnLogs
      .map(conlog => (conlog.connect_type,1))
      .reduceByKeyAndWindow( _ + _ , _ -_ , bWindowDuration.value,bSlideDuration.value)
      .transform(skipEmptyWordCount)  //Uncommnet this line to remove empty wordcoutn such as : SignInt : Count 0
    /*
    Accumulative count
    */
    val accumutiveCounting = new ConcurrentHashMapAccumulator()
    sc.register(accumutiveCounting)
      connType.foreachRDD { rdd =>
        //val spark = SparkSession.builder().getOrCreate()
        //val acc = mc.getInstance(sc)
        //val acc = mc.getInstance(spark.sparkContext)
        if(!rdd.isEmpty()) {
          val context = rdd.sparkContext
          // val acc = mc.getInstance(context) //////// Raise of null point exception.
          rdd.foreach{case(word,count) => accumutiveCounting add(word -> count)}
          val accValue: mutable.Map[String, Int] = accumutiveCounting.value
          val accRDD: RDD[StatusCount] = context.parallelize(accValue.toSeq).map{case(typeName,count)=> StatusCount(typeName,count,getCurrentTime())}
          val accDF                    = accRDD.toDF()
          //println(accValue)
          accDF.write.mode(SaveMode.Overwrite).mongo(WriteConfig(Map("collection"->"connlog_accumulative_count"),Some(WriteConfig(context))))
          //MongoSpark.save(accDF.write.option("collection", "connlog_accumulative_count").mode("overwrite"))
        }
      }


    connType.transform(toWouldCountObject)
      .foreachRDD{rdd =>
      val data = rdd.toDF()
        // Write config should put in SparkConfig.
        //This still a reliable way to write from a stream to mongo.
      data.write.mode(SaveMode.Append).mongo()
        //Mongo Spark Connector 2.0 API :
        //@since 2017-03-22
        //MongoSpark.save(data.write.option("collection","collectionName").mode("overwrite"))
    }
    
      //rdd.map()
    }
  def toLowerCase   = (lines: RDD[String]) => lines.map(words => words.toLowerCase)
  def extractValue  = (parser: Broadcast[ConnLogParser]) => (lines: RDD[String]) =>
    lines.map(line => parser.value.extractValues(line).get.asInstanceOf[ConnLogLineObject])
  def toWouldCountObject = (streams : RDD[(String,Int)]) => streams.map(tuple => StatusCount(tuple._1.toString, tuple._2,getCurrentTime()))
  def getCurrentTime():String ={
    val now = Calendar.getInstance().getTime
    val nowFormater = new SimpleDateFormat("yyyy/MM/dd - hh:mm:ss")
    val nowFormeted = nowFormater.format(now).toString
    return nowFormeted
  }
  def skipEmptyWordCount = (streams : RDD[(String,Int)]) => streams.filter(wordCount => wordCount._2 > 0)
  }

case class StatusCount(connType: String,count: Int,time: String) extends Serializable{}

/*
object dateTimeTest{
  def main(args: Array[String]): Unit = {
    val now = Calendar.getInstance()
    println(getCurrentTime)
  }
  def getCurrentTime():String ={
    val now = Calendar.getInstance().getTime
    val nowFormater = new SimpleDateFormat("yyyy/MM/dd - hh:mm:ss")
    val nowFormeted = nowFormater.format(now).toString
    return nowFormeted
  }
}
*/

