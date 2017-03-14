package com.hungsiro.spark_kafka.core.sinks
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import com.hungsiro.spark_kafka.core.KafkaPayLoad
import com.hungsiro.spark_kafka.core.KafkaProducerFactory
import sinks.KafkaDStreamSinkExceptionHandler
import org.apache.log4j.Logger

import scala.reflect.ClassTag
/**
  * Created by hungdv on 14/03/2017.
  */
class RDDKafkaWriter[T: ClassTag](@transient private val rdd: RDD[T] )
  extends KafkaWrite[T] with Serializable {

  override def sendToKafka[K, V](
                                  producerConfig: Map[String, Object],
                                  topic: String,
                                  transformFunc: T => ProducerRecord[K, V]): Unit ={

    rdd.foreachPartition { partition =>
      val producer: KafkaProducer[K, V] = KafkaProducerFactory.getOrCreateProducer(producerConfig)
      val context = TaskContext.get
      val callback = new KafkaDStreamSinkExceptionHandler
      val logger = Logger.getLogger(getClass)
      logger.debug(s"Send Spark partition: ${context.partitionId()} to Kafka topic : $topic")

      val metadata = partition.map(transformFunc).map{ record =>
        callback.throwExceptionIfAny()
        producer.send(record,callback)
      }.toList

    }


  }
}
