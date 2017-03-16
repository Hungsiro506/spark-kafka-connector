# Spark - Kafka Connector
## Features:
* Caching producer in executor and share with all JVM tasks
* Shutdown hook to close producer when Spark executor is shutdown
* Generic type for Kafka payload
* Async sending msg to Kafka from SparkStreaming (Reciever and non-reciever)
## Usage

If you want to save an RDD to Kafka
```sh
import com.hungsiro.spark_kafka.core.sink._
import com.hungsiro.spark_kafka.core.source._
import org.apache.kafka.common.serialization.StringSerializer

val topic = "my-topic"
val producerConfig: Map[String,Object] = loadConfig().producerConfig

val rdd: RDD[String] = ...
rdd.sendToKafka(
  producerConfig,
  s => new ProducerRecord[String, String](topic, s)
  //  s => new ProducerRecord[Array[Byte],Array[Byte]](config.outputTopic,s.key.toString.getBytes(),s.value.toString.getBytes())
)
```
If you want to save a DStream to Kafka
```sh
import com.hungsiro.spark_kafka.core.sink._
import com.hungsiro.spark_kafka.core.source._
val topic = "my-topic"
val producerConfig: Map[String,Object] = loadConfig().producerConfig

val dStream: DStream[String] = ...
dStream.sendToKafka(
  producerConfig,
  s => new ProducerRecord[String, String](topic, s)
  // //  s => new ProducerRecord[Array[Byte],Array[Byte]](config.outputTopic,s.key.toString.getBytes(),s.value.toString.getBytes())
)
```
## Example :
### Start ZooKeeper server:
```sh
./bin/zookeeper-server-start.sh config/zookeeper.properties
```
### Start Kafka server:
```sh
./bin/kafka-server-start.sh config/server.properties
```
### Create input topic:
```sh
./bin/kafka-topics.sh --create --zookeeper localhost:2182 --replication-factor 1 --partitions 1 --topic input
```
### Create output topic:
```sh
./bin/kafka-topics.sh --create --zookeeper localhost:2182 --replication-factor 1 --partitions 1 --topic output
```
### Start Kafka producer:
```sh
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input
```
### Start Kafka consumer:
```sh
./bin/kafka-console-consumer.sh --zookeeper localhost:2182 --topic output
```
Run example application and publish a few words on input topic using Kafka console producer and check the processing result on output topic using Kafka console producer.


