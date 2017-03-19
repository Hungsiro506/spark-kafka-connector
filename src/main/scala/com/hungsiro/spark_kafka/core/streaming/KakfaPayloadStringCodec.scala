package com.hungsiro.spark_kafka.core.streaming
import com.hungsiro.spark_kafka.core.KafkaPayLoad
import com.twitter.bijection.{Injection, StringCodec}
import org.apache.log4j.Logger

import scala.util.{Failure, Success}


/**
  * Created by hungdv on 13/03/2017.
  */
class KafkaPayloadStringCodec extends Serializable{



}
object KafkaPayloadStringCodec {
  @transient lazy private val logger = Logger.getLogger(getClass)
  @transient lazy implicit private val stringInjection = StringCodec.utf8
  def apply(): KafkaPayloadStringCodec = new KafkaPayloadStringCodec

  private def decodeValue[K,V](payload: KafkaPayLoad[K,V])(implicit vInj: Injection[String, V]): Option[String] = {

    val decodedValueTry = Injection.invert[String, V](payload.value)
    decodedValueTry match {
      case Success(record) =>
        Some(record)
      case Failure(ex) =>
        logger.warn("Could not decode value of payload", ex)
        None
    }

  }
  private def decodeKey[K,V](payload: KafkaPayLoad[K,V])(implicit kInj: Injection[String, K]): Option[String] = {

    val decodedValueTry = Injection.invert[String, K](payload.key)
    decodedValueTry match {
      case Success(record) =>
        Some(record)
      case Failure(ex) =>
        logger.warn("Could not decode value of payload", ex)
        None
    }

  }

  /**
    * Decode Kafka Payload with full key-value
    * @param payload Payload with Key-Value pair
    * @tparam K Key Type
    * @tparam V Value Type
    * @return (Option[String],Option[String]) concreate (String,String) From KafkaPayload
    */
  def decodePayload[K,V](payload: KafkaPayLoad[K,V])(implicit kInj: Injection[String, K], vInj: Injection[String, V]): (Option[String], Option[String]) ={
    return (decodeKey(payload),decodeValue(payload))
  }

  /**
    * Decode Kafka Payload with key only and neglect key
    * @param payload Payload without Key.
    * @tparam K Key Type
    * @tparam V Value Type
    * @return Option[String] Concreate String of value from Kafka payload
    */
  def decodePayload[K,V](payload: KafkaPayLoadWithNoneKey[K,V])(implicit vInj: Injection[String, V]): Option[String] = {
    val decodedValueTry = Injection.invert[String, V](payload.value)
    decodedValueTry match {
      case Success(record) =>
        Some(record)
      case Failure(ex) =>
        logger.warn("Could not decode value of payload", ex)
        None
    }
  }

  /**
    * Encode String to Kafka Payload value only
    * @param value
    * @tparam K
    * @tparam V
    * @return Kafka Payload Without key
    */
  def encode[K,V](value: String)(implicit vInj: Injection[String, V]): KafkaPayLoadWithNoneKey[K,V] = {
    val encoded = Injection[String, V](value)
    KafkaPayLoadWithNoneKey(None, encoded)
  }

  /**
    * Encode String-String to Kafka Payload key-value pair
    * @param key
    * @param value
    * @tparam K
    * @tparam V
    * @return KafkaPayLoad
    */
  def encode[K,V](key: String,value: String)(implicit kInj: Injection[String, K], vInj: Injection[String, V]): KafkaPayLoad[K,V] = {
    val encodedValue = Injection[String, V](value)
    val encodedKey = Injection[String,K](key)
    KafkaPayLoad(encodedKey, encodedValue)
  }

}

