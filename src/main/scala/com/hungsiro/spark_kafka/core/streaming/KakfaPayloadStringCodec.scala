package com.hungsiro.spark_kafka.core.streaming
import com.hungsiro.spark_kafka.core.KafkaPayLoad
import com.twitter.bijection.{Injection, StringCodec}
import org.apache.log4j.Logger

import scala.util.{Failure, Success}


/**
  * Created by hungdv on 13/03/2017.
  */
class KafkaPayloadStringCodec extends Serializable{
  @transient lazy private val logger = Logger.getLogger(getClass)
  @transient lazy implicit private val stringInjection = StringCodec.utf8

/*  def decodeValue(payload: KafkaPayLoad): Option[String] = {
    val decodedTry = Injection.invert[String, Array[Byte]](payload.value)
    decodedTry match {
      case Success(record) =>
        Some(record)
      case Failure(ex) =>
        logger.warn("Could not decode payload", ex)
        None
    }
  }

  def encodeValue(value: String): KafkaPayLoad = {
    val encoded = Injection[String, Array[Byte]](value)
    KafkaPayLoad(None, encoded)
  }*/

}


object KafkaPayloadStringCodec {
  def apply(): KafkaPayloadStringCodec = new KafkaPayloadStringCodec
}

