package core

/**
  * Created by hungdv on 10/03/2017.
  */
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import scala.collection.mutable
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaProducerFactory {
  import scala.collection.JavaConversions._

  private val logger = Logger.getLogger(getClass)

  private val producer = mutable.Map[Map[String,Object],KafkaProducer[String,String]]()

  def getOrCreateProducer(config: Map[String,Object]): KafkaProducer[String,String]  = {
    val defaulConfig = Map(
      "key.serializer" -> classOf[StringDeserializer],
      "value.serializer" -> classOf[StringDeserializer]
    )

    val finalConfig = defaulConfig ++ config

    producer.getOrElseUpdate(finalConfig,{
      logger.info(s"Create Kafka producer , config: $finalConfig")
      val producer = new KafkaProducer[String,String](finalConfig)

      sys.addShutdownHook{
        logger.info(s"Close Kafka producer, config: $finalConfig")
        producer.close()
      }

      producer
    })
  }
}
