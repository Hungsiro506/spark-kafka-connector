package com.hungsiro.spark_kafka.core.streaming

import com.hungsiro.spark_kafka.core.Payload

/**
  * Created by hungdv on 15/03/2017.
  */

case class GenericKafkaPayload[K,V](key: K, value: V) extends Payload[K,V]{
}
