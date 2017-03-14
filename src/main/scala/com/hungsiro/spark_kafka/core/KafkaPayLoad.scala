package com.hungsiro.spark_kafka.core

/**
  * Created by hungdv on 10/03/2017.
  */
case class KafkaPayLoad(key: Option[Array[Byte]], value: Array[Byte]) extends Serializable{

}
