package sinks

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.producer.KafkaProducer

/**
  * Created by hungdv on 09/03/2017.
  */
/*
class KafkaSink(createProducer: ()=> KafkaProducer[String,String])extends Serializable{
  lazy val producer = createProducer()

  def sent(topic: String, value:String): Unit = producer.send(new ProducerRecord[String,String](topic,value))
}


object KafkaSink {
  def apply(configs: Map[String, Object]): KafkaSink = {
    val f = () => {
      val producer = new org.apache.kafka.clients.producer.KafkaProducer[String,String](configs)

      sys.addShutdownHook {
        producer.close()
      }
      producer
      //new KafkaProducer[String,String](config)
    }
    new KafkaSink(f)
  }
}*/
