package app

/**
  * Created by hungdv on 10/03/2017.
  */
import com.hungsiro.spark_kafka.core.KafkaPayLoad
import org.apache.spark.storage.StorageLevel
import com.hungsiro.spark_kafka.core.sinks._
import com.hungsiro.spark_kafka.core.sources._
import com.hungsiro.spark_kafka.core.streaming._
import org.apache.spark.streaming.dstream.DStream
import sources.KafkaDStreamSource
import streaming.SparkStreamingApplication

import scala.concurrent.duration.FiniteDuration

class WordCountJob(config: WordCountJobConfig, source: KafkaDStreamSource) extends SparkStreamingApplication {

  override def sparkConfig: Map[String, String] = config.spark

  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckpointDir

  def start(): Unit = {
    withSparkStreamingContext { (ss, ssc) =>
      val input: DStream[KafkaPayLoad] = source.createSource(ssc, config.inputTopic)

      // Option 1: Array[Byte] -> String
      val sc = ss.sparkContext
      //val stringCodec = sc.broadcast(KafkaPayloadStringCodec())
      //val lines = input.flatMap(stringCodec.value.decodeValue(_))
      val lines: DStream[String] = input.map(kafkaPayload => kafkaPayload.value.toString)

      // Option 2: Array[Byte] -> Specific Avro
      //val avroSpecificCodec = sc.broadcast(KafkaPayloadAvroSpecificCodec[SomeAvroType]())
      //val lines = input.flatMap(avroSpecificCodec.value.decodeValue(_))

      val countedWords: DStream[(String, Int)] = WordCount.countWords(
        ssc,
        lines,
        config.stopWords,
        config.windowDuration,
        config.slideDuration
      )

      // encode Kafka payload (e.g: to String or Avro)
      val output: DStream[String] = countedWords
        .map(_.toString())


      // cache to speed-up processing if action fails
      output.persist(StorageLevel.MEMORY_ONLY_SER)

      import sinks.KafkaDStreamSink._
      output.sendToKafka(config.sinkKafka, config.outputTopic)
    }
  }

}

object WordCountJob {

  def main(args: Array[String]): Unit = {
    val config = WordCountJobConfig()

    val streamingJob = new WordCountJob(config, KafkaDStreamSource(config.sourceKafka))
    streamingJob.start()
  }

}

case class WordCountJobConfig(
                               inputTopic: String,
                               outputTopic: String,
                               stopWords: Set[String],
                               windowDuration: FiniteDuration,
                               slideDuration: FiniteDuration,
                               spark: Map[String, String],
                               streamingBatchDuration: FiniteDuration,
                               streamingCheckpointDir: String,
                               sourceKafka: Map[String, String],
                               sinkKafka: Map[String, String])
  extends Serializable

object WordCountJobConfig {

  import com.typesafe.config.{Config, ConfigFactory}
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader.arbitraryTypeValueReader


  def apply(): WordCountJobConfig = apply(ConfigFactory.load)

  def apply(applicationConfig: Config): WordCountJobConfig = {

    val config: Config = applicationConfig.getConfig("wordCountJob")

    new WordCountJobConfig(
      config.as[String]("input.topic"),
      config.as[String]("output.topic"),
      config.as[String]("stopWords").asInstanceOf[Set[String]],
      config.as[FiniteDuration]("windowDuration"),
      config.as[FiniteDuration]("slideDuration"),
      config.as[Map[String, String]]("spark"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckpointDir"),
      config.as[Map[String, String]]("kafkaSource"),
      config.as[Map[String, String]]("kafkaSink")

    )
  }

}
