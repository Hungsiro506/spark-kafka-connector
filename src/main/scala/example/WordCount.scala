package example

/**
  * Created by hungdv on 10/03/2017.
  */

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.concurrent.duration.FiniteDuration

object WordCount {

  type WordCount = (String, Int)

  def countWords(
                  ssc: StreamingContext,
                  lines: DStream[String],
                  stopWords: Set[String],
                  windowDuration: FiniteDuration,
                  slideDuration: FiniteDuration): DStream[WordCount] = {

    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)

    val sc = ssc.sparkContext

    val stopWordsVar = sc.broadcast(stopWords)
    val windowDurationVar = sc.broadcast(windowDuration)
    val slideDurationVar = sc.broadcast(slideDuration)

    val words = lines
      .transform(splitLine)
      .transform(skipEmptyWords)
      .transform(toLowerCase)
      .transform(skipStopWords(stopWordsVar))

    val wordCounts = words
      .map(word => (word, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, windowDurationVar.value, slideDurationVar.value)

    wordCounts
      .transform(skipEmptyWordCounts)
      .transform(sortWordCounts)
  }

  val toLowerCase = (words: RDD[String]) => words.map(word => word.toLowerCase)

  val splitLine = (lines: RDD[String]) => lines.flatMap(line => line.split("[^\\p{L}]"))

  val skipEmptyWords = (words: RDD[String]) => words.filter(word => !word.isEmpty)

  val skipStopWords = (stopWords: Broadcast[Set[String]]) => (words: RDD[String]) =>
    words.filter(word => !stopWords.value.contains(word))

  val skipEmptyWordCounts = (wordCounts: RDD[WordCount]) => wordCounts.filter(wordCount => wordCount._2 > 0)

  val sortWordCounts = (wordCounts: RDD[WordCount]) => wordCounts.sortByKey()

}
