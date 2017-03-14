package com.hungsiro.spark_kafka.core.sinks

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag
/**
  * Created by hungdv on 14/03/2017.
  */
abstract class KafkaWrite[T: ClassTag] extends Serializable {
  def sendToKafka[K,V](
                 producerConfig: Map[String,Object],
                 topic: String,
                 transformFunc: T => ProducerRecord[K,V]
                 ): Unit
}

