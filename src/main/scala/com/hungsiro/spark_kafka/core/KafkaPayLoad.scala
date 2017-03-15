package com.hungsiro.spark_kafka.core

import com.hungsiro.spark_kafka.core.streaming.Payload

/**
  * Created by hungdv on 10/03/2017.
  */
case class KafkaPayLoad[K,V](key: K, value: V) extends Payload[K,V]{

}
