package spark_kafka_connector_scala_test

/**
  * Created by hungdv on 03/03/2017.
  */

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random
import scala.util.control.Breaks

object BicycleDataProducer {
  def main(args: Array[String]): Unit = {

    //Get the Kafka broker node
    val brokers = scala.util.Try(args(0)).getOrElse("localhost:9092")

    //Get the exists topic named welcome-message
    val topic = scala.util.Try(args(1)).getOrElse("bike-data")

    val events = scala.util.Try(args(2)).getOrElse("0").toInt

    //val intervalEvent = util.Try(args(3)).getOrElse("1").toInt //in second

    val intervalEvent = scala.util.Try(args(3)).getOrElse("0").toInt //in second

    val rndStart = scala.util.Try(args(4)).getOrElse("0").toInt //in second

    val rndEnd = scala.util.Try(args(5)).getOrElse("500").toInt //in second

    val clientId = UUID.randomUUID().toString()

    //Create some properties
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", clientId)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    println("======================BEGIN=============================")

    val bicyclesSrc = List("Trek", "Giant", "Jett", "Cannondale", "Surly")
    val rnd = new Random()
    val rnd2 = new Random()

    var i = 0

    val loop = new Breaks()

    //The while loop will generate the data and send to Kafka
    loop.breakable{
      while(true){

        val n = rndStart + rnd2.nextInt(rndEnd - rndStart + 1)
        for(i <- Range(0, n)){
          val today = Calendar.getInstance.getTime
          val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val key = UUID.randomUUID().toString().split("-")(0)
          val value = formatter.format(today) + "," + bicyclesSrc(rnd.nextInt(bicyclesSrc.length))
          val data = new ProducerRecord[String, String](topic, key, value)

          println("--- topic: " + topic + " ---")
          println("key: " + data.key())
          println("value: " + data.value() + "\n")
          producer.send(data)
        }

        val k = i + 1
        println(s"--- #$k: $n records in [$rndStart, $rndEnd] ---")

        if(intervalEvent > 0)
          Thread.sleep(intervalEvent * 1000)

        i += 1
        if(events > 0 && i == events)
          loop.break()
      }
    }
  }
}
