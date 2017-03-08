package spark_kafka_connector_java_test

import consumer.kafka.{ProcessedOffsetManager, ReceiverLauncher}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext};

/**
  * Created by hungdv on 27/02/2017.
  */
class Example {

}
object example{
  def main(args: Array[String]):Unit ={
    var conf  = new SparkConf()
      .setMaster("local")
      .setAppName("Spark-Kafka-Connector-Exp")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction", "1")
      .set("spark.streaming.unpersist", "true")

    val sc = new SparkContext(conf)
    //Might want to uncomment and add the jars if you are running on standalone mode.
    //sc.addJar("/home/workspace/kafka-spark-consumer/target/kafka-spark-consumer-1.0.10-jar-with-dependencies.jar")
    val ssc = new StreamingContext(sc, Seconds(10))

    val topic = "test4"
    val zkhosts = "localhost"
    val zkports = "2182"

    //Specify number of Receivers you need.
    val numberOfReceivers = 1
    val kafkaProperties: Map[String, String] =
      Map("zookeeper.hosts" -> zkhosts,
        "zookeeper.port" -> zkports,
        "kafka.topic" -> topic,
        "zookeeper.consumer.connection" -> "localhost:2182",
        "kafka.consumer.id" -> "kafka-consumer",
        //optional properties
        "consumer.fetchsizebytes" -> "1048576",
        "consumer.fillfreqms" -> "1000",
        "consumer.num_fetch_to_buffer" -> "1")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key,value) => props.put(key, value)}

    val tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers,StorageLevel.MEMORY_ONLY)
    //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
    val partitonOffset_stream = ProcessedOffsetManager.getPartitionOffset(tmp_stream, props)

    //Start Application Logic
    tmp_stream.foreachRDD(rdd => {
      println("\n\nNumber of records in this batch : " + rdd.count())
    } )

    //End Application Logic

    //Persists the Max Offset of given Kafka Partition to ZK
    ProcessedOffsetManager.persists(partitonOffset_stream, props)
    ssc.start()
    ssc.awaitTermination()

  }
}
