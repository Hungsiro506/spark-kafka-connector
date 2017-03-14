package com.hungsiro.spark_kafka.core

/**
  * Created by hungdv on 10/03/2017.
  */
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import scala.collection.mutable
import org.apache.kafka.common.serialization.{ByteArraySerializer,ByteArrayDeserializer}

object KafkaProducerFactory {
  import scala.collection.JavaConversions._

  private val logger = Logger.getLogger(getClass)

  private val producer = mutable.Map[Map[String,Object],KafkaProducer[Array[Byte], Array[Byte]]]()

  def getOrCreateProducer(config: Map[String,Object]): KafkaProducer[Array[Byte], Array[Byte]]  = {
    val defaulConfig = Map(
     /* "key.serializer" -> classOf[ByteArraySerializer],
      "value.serializer" -> classOf[ByteArraySerializer]*/
      "key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
    )

    val finalConfig = defaulConfig ++ config

    producer.getOrElseUpdate(finalConfig,{
      logger.info(s"Create Kafka producer , config: $finalConfig")
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](finalConfig)

      sys.addShutdownHook{
        logger.info(s"Close Kafka producer, config: $finalConfig")
        producer.close()
      }

      producer
    })
  }
}
