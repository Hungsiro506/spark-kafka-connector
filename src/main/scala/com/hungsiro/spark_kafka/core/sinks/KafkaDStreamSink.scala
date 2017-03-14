package sinks
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.streaming.dstream.DStream
import com.hungsiro.spark_kafka.core.KafkaPayLoad
import com.hungsiro.spark_kafka.core.KafkaProducerFactory
import com.hungsiro.spark_kafka.core.sinks.{KafkaWrite, RDDKafkaWriter}
import org.apache.log4j.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
/**
  * Created by hungdv on 10/03/2017.
  */
class KafkaDStreamSink[T: ClassTag](@transient private val dstream: DStream[T])
  extends KafkaWrite[T] with Serializable{

  override  def sendToKafka[K,V](
                        config: Map[String,Object],
                        topic: String,
                        transfromFunc: T => ProducerRecord[K,V]): Unit ={
//  def sendToKafka(config: Map[String,Object],topic: String,producerBroadcast: Broadcast[KafkaProducer[Array[Byte],Array[Byte]]]):Unit ={
 /* def sendToKafka(configs: Map[String,Object],topic: String,sc: SparkContext):Unit ={
    val sink: KafkaProducer[Array[Byte], Array[Byte]] = KafkaProducerFactory.getOrCreateProducer(configs)
    val producer: Broadcast[KafkaProducer[Array[Byte], Array[Byte]]] = sc.broadcast(sink)*/

    dstream.foreachRDD{ rdd =>
      /*rdd.foreachPartition{ records =>

        val producer = KafkaProducerFactory.getOrCreateProducer(config)
        //val producer = producerBroadcast.value

        val context = TaskContext.get
        val logger  = Logger.getLogger(getClass)

        val callback = new KafkaDStreamSinkExceptionHandler
        logger.debug(s"Send Spark partition: ${context.partitionId()} to Kafka topic : $topic")

        val metadata = records.map{ record =>
          callback.throwExceptionIfAny()
          producer.send(new ProducerRecord(topic,record.key.orNull,record.value), callback)
          /*producer.value.send(new ProducerRecord(topic,record.key.orNull,record.value), callback)*/
        }.toList

        logger.debug(s"Flush Spark partition: ${context.partitionId} to Kafka topic: $topic")
        metadata.foreach{
          metadata => metadata.get()
        }

        callback.throwExceptionIfAny()
      }*/
      val rddWriter = new RDDKafkaWriter[T](rdd)
      rddWriter.sendToKafka(config,topic,transfromFunc)
    }
  }
}

object KafkaDStreamSink{
  import scala.language.implicitConversions

  implicit def createKafkaDStreamSink[T: ClassTag, K,V](dStream: DStream[T]): KafkaWrite[T] = {
    new KafkaDStreamSink[T](dStream)
  }
  implicit def createKafkaRDDSink[T:ClassTag,K,V](rdd: RDD[T]): KafkaWrite[T] = {
    new RDDKafkaWriter[T](rdd)
  }
}