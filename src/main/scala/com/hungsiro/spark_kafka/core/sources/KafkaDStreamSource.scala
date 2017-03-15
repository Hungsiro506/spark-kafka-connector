package sources
import com.hungsiro.spark_kafka.core.KafkaPayLoad
import kafka.serializer.DefaultDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.hungsiro.spark_kafka.core.streaming.Payload
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import scala.reflect.ClassTag
/**
  * Created by hungdv on 10/03/2017.
  */
class KafkaDStreamSource(configs: Map[String,Object]) extends Serializable{

  def createSource[K,V](scc: StreamingContext,topic: String,
                        transformFunc: ConsumerRecord[K,V] => Payload[K,V]  ): DStream[Payload[K,V]] ={
    val kafkaParams = configs
    val kafkaTopics = Set(topic)

    KafkaUtils.createDirectStream[K,V](
      scc,
      PreferConsistent, //locationStrategy
      Subscribe[K,V](kafkaTopics,kafkaParams) //consumerStrategy
    ).map(transformFunc)
  }
  def createSource[K,V](scc: StreamingContext,topic: String ): DStream[(K,V)] ={
    val kafkaParams = configs
    val kafkaTopics = Set(topic)

    KafkaUtils.createDirectStream[K,V](
      scc,
      PreferConsistent, //locationStrategy
      Subscribe[K,V](kafkaTopics,kafkaParams) //consumerStrategy
    ).map(dstream => (dstream.key,dstream.value))
  }

  /**
    * Simple String DStream From Source. Will be removed soon.
    * @param scc
    * @param topic
    * @return
    */
  def createSourceSimple(scc: StreamingContext,topic: String ): DStream[String] ={
    val kafkaParams = configs
    val kafkaTopics = Set(topic)

    KafkaUtils.createDirectStream[String,String](
      scc,
      PreferConsistent, //locationStrategy
      Subscribe[String,String](kafkaTopics,kafkaParams) //consumerStrategy
    ).map(dstream => dstream.value.toString)
  }
}
object KafkaDStreamSource{
  def apply(configs: Map[String, String]): KafkaDStreamSource = new KafkaDStreamSource(configs)
}