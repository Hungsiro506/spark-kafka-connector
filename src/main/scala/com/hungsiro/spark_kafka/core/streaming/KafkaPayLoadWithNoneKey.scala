package com.hungsiro.spark_kafka.core.streaming

import com.hungsiro.spark_kafka.core.Payload

/**
  * Created by hungdv on 15/03/2017.
  */
case class KafkaPayLoadWithNoneKey[K,V](key: Option[K], value: V) extends Payload[K,V] {
}