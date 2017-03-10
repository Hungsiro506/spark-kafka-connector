package sinks
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream.DStream
import core.KafkaPayLoad
import core.KafkaProducerFactory
import org.apache.log4j.Logger
import org.apache.kafka.clients.producer.{KafkaProducer,ProducerRecord,RecordMetadata}
/**
  * Created by hungdv on 10/03/2017.
  */
class KafkaDStreamSink(dstream: DStream[KafkaPayLoad]) {
  def sendToKafka(configs: Map[String,Object],topic: String):Unit ={
    dstream.foreachRDD{ rdd =>
      rdd.foreachPartition{ records =>
        val producer = KafkaProducerFactory.getOrCreateProducer(configs)

        val context = TaskContext.get
        val logger  = Logger.getLogger(getClass)

        val callback = new KafkaDStreamSinkExceptionHandler
        logger.debug(s"Send Spark partition: ${context.partitionId()} to Kafka topic : $topic")

        val metadata = records.map{ record =>
          callback.throwExceptionIfAny()
          producer.send(new ProducerRecord(topic,record.key.orNull,record.value), callback)
        }.toList

        logger.debug(s"Flush Spark partition: ${context.partitionId} to Kafka topic: $topic")
        metadata.foreach{
          metadata => metadata.get()
        }

        callback.throwExceptionIfAny()
      }

    }
  }
}

object KafkaDStreamSink{
  import scala.language.implicitConversions

  implicit def createKafkaDStreamSink(dsStream: DStream[KafkaPayLoad]): KafkaDStreamSink = {
    new KafkaDStreamSink(dsStream)
  }
}