package app

/**
  * Created by hungdv on 10/03/2017.
  */
import com.hungsiro.spark_kafka.core.KafkaPayLoad
import com.hungsiro.spark_kafka.core.streaming.KafkaPayloadStringCodec
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import sources.KafkaDStreamSource
import streaming.SparkStreamingApplication
import com.hungsiro.spark_kafka.core.KafkaProducerFactory
import org.apache.kafka.clients.producer.ProducerRecord
import com.hungsiro.spark_kafka.core.sinks._
import org.apache.kafka.common.serialization.StringSerializer


import scala.concurrent.duration.FiniteDuration

class WordCountJob(config: WordCountJobConfig, source: KafkaDStreamSource) extends SparkStreamingApplication {

  override def sparkConfig: Map[String, String] = config.spark

  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckpointDir

  def start(): Unit = {
    //withSparkStreamingContext { (ss, ssc) =>
    withSparkStreamingContext { (sc, ssc) =>

    //val input: DStream[KafkaPayLoad] = source.createSource(ssc, config.inputTopic)
      val input: DStream[String]  = source.createSourceSimple(ssc, config.inputTopic)

      // Option 1: Array[Byte] -> String

      //val sc = ss.sparkContext

      //val stringCodec = sc.broadcast(KafkaPayloadStringCodec())
      //val stringCodec = sc.broadcast(KafkaPayloadStringCodec())
      //val lines = input.flatMap(stringCodec.value.decodeValue(_))
      //val lines: DStream[String] = input.map(kafkaPayload => kafkaPayload.value.toString)
      //val lines = input.flatMap(_)
      val lines = input
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

/*      // encode Kafka payload (e.g: to String or Avro)
      val output: DStream[KafkaPayLoad] = countedWords
        .map(_.toString())
        .map(stringCodec.value.encodeValue(_))


      // cache to speed-up processing if action fails
      output.persist(StorageLevel.MEMORY_ONLY_SER)

      import sinks.KafkaDStreamSink._
      //output.sendToKafka(config.sinkKafka, config.outputTopic,sc)
      //val producer = KafkaProducerFactory.getOrCreateProducer(config.sinkKafka)
      //val producerBroadast = sc.broadcast(producer)
      //output.sendToKafka(config.sinkKafka, config.outputTopic,producerBroadcast)
      // Broadcast bi loi  com.esotericsoftware.kryo.KryoException: java.util.ConcurrentModificationException
      output.sendToKafka(config.sinkKafka,
                        config.outputTopic,
                        //s => new ProducerRecord[String,String](config.outputTopic,s.key.toString,s.value.toString))
                        s => new ProducerRecord[Array[Byte], Array[Byte]](config.outputTopic, s.key.toString.getBytes(),s.value.toString.getBytes()))*/
      val output: DStream[(String,String)] = countedWords.map(payload => (payload._1,payload._2.toString))
        //.map(_.toString())
        //.map(stringCodec.value.encodeValue(_)).map(payload => (payload.key.toString,payload.value.toString))
        //map(payload => (payload.key.toString,payload.value.toString))


      output.persist(StorageLevel.MEMORY_ONLY)
      import sinks.KafkaDStreamSink._
      output.sendToKafka(config.sinkKafka,
        config.outputTopic,
        s=> new ProducerRecord[String,String](config.outputTopic,s._1,s._2))
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
                               sinkKafka: Map[String, String]) extends Serializable

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
      config.as[Set[String]]("stopWords"),
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
