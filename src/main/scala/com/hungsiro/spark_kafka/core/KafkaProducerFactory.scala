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

  private val producers = mutable.HashMap.empty[Map[String,Object], KafkaProducer[_, _]]

  def getOrCreateProducer[K,V](config: Map[String,Object]): KafkaProducer[K, V] = {

    //Should remove this config
    val defaulConfig = Map(
     /* "key.serializer" -> classOf[ByteArraySerializer],
      "value.serializer" -> classOf[ByteArraySerializer]*/
      //"key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      //"value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
    )

    val finalConfig = defaulConfig ++ config


    producers.getOrElseUpdate(finalConfig,{
      logger.info(s"Create Kafka producer , config: $finalConfig")
      val producer = new KafkaProducer[K,V](finalConfig)
      producers(finalConfig) = producer
      sys.addShutdownHook{
        logger.info(s"Close Kafka producer, config: $finalConfig")
        producer.close()
      }
      producer
    }).asInstanceOf[KafkaProducer[K,V]]
  }
}
