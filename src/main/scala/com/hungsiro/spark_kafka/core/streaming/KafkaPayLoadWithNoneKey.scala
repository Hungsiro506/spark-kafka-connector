package com.hungsiro.spark_kafka.core.streaming

/**
  * Created by hungdv on 15/03/2017.
  */
case class KafkaPayLoadWithNoneKey[K,V](key: Option[K], value: V) extends Payload[K,V] {

}
