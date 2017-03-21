package storage.es

import example.WordCount
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration
import org.elasticsearch.spark._

/**
  * Created by hungdv on 17/03/2017.
  */
object ESTest{
  // type khong phai case class!!!!!!
  type WordCount = (String,Int)

  def main(args: Array[String]): Unit ={
    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)
    val sparkConfig = new SparkConf()
    sparkConfig.set("es.port","9200")
      .set("es.nodes","localhost")
      .set("es.http.timeout","5m")
      .set("es.scroll.size","50")
      .set("es.index.auto.create", "true")
      .setMaster("local[2]")
      .setAppName("EsTest")


    val ss: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
    val sc: SparkContext = ss.sparkContext
    val streamingContext = new StreamingContext(sc,Seconds(5))
    val dStream : DStream[String] = streamingContext.socketTextStream("localhost", 9999)
    val words = dStream
      .transform(splitLine)
      .transform(skipEmptyWords)
      .transform(toLowerCase)

    val windowDuration = new FiniteDuration(5,java.util.concurrent.TimeUnit.SECONDS)
    val slideDuration = new FiniteDuration(5,java.util.concurrent.TimeUnit.SECONDS)
    val windowDurationVar = sc.broadcast(windowDuration)
    val slideDurationVar = sc.broadcast(slideDuration)

    val wordCounts: DStream[(String, Int)] = words
      .map(word => (word, 1))
      .reduceByKeyAndWindow(_ + _, _ - _,windowDurationVar.value,slideDurationVar.value)
      // transform tren DStream nhan vao mot map func or mot trans func.
      .transform(sortWordCounts)


    import output_storage.es.ElasticSearchDStreamWriter._
    wordCounts.persistToStorage(Map[String,String]("index" -> "sparkESTest","type" -> "caseClass"))

    //dStream.persistToStorage(Map[String,String]("index" -> "test","type" -> "typeTest"))
    dStream.foreachRDD{rdd =>
      val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
      //val counter = Map("signIn" -> singInCount,"logOff" -> logOffCount,"reject"  -> rejectCount)

      val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

      sc.makeRDD(Seq(numbers, airports)).saveToEs("spark_es_test/docs")
      rdd.saveToEs("test/radius")
    }
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  val toLowerCase = (words: RDD[String]) => words.map(word => word.toLowerCase)

  val splitLine = (lines: RDD[String]) => lines.flatMap(line => line.split("[^\\p{L}]"))

  val skipEmptyWords = (words: RDD[String]) => words.filter(word => !word.isEmpty)

  val skipStopWords = (stopWords: Broadcast[Set[String]]) => (words: RDD[String]) =>
    words.filter(word => !stopWords.value.contains(word))

  val sortWordCounts = (wordCounts: RDD[WordCount]) => wordCounts.sortByKey()



}