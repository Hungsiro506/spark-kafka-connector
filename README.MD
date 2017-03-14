Usage

if you want to save an RDD to Kafka


import com.hungsiro.spark_kafka.core.sink._
import com.hungsiro.spark_kafka.core.source._
import org.apache.kafka.common.serialization.StringSerializer

val topic = "my-topic"
val producerConfig: Map[String,Object] = loadConfig().producerConfig

val rdd: RDD[String] = ...
rdd.sendToKafka(
  producerConfig,
  s => new ProducerRecord[String, String](topic, s)
)
if you want to save a DStream to Kafka
import java.util.Properties

// replace by kafka08 if you're using Kafka 0.8
import com.github.benfradet.spark.kafka010.writer._
import org.apache.kafka.common.serialization.StringSerializer

val topic = "my-topic"
val producerConfig: Map[String,Object] = loadConfig().producerConfig

val dStream: DStream[String] = ...
dStream.sendToKafka(
  producerConfig,
  s => new ProducerRecord[String, String](topic, s)
)


Start ZooKeeper server:

./bin/zookeeper-server-start.sh config/zookeeper.properties
Start Kafka server:

./bin/kafka-server-start.sh config/server.properties
Create input topic:

./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic input
Create output topic:

./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic output
Start Kafka producer:

./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input
Start Kafka consumer:

./bin/kafka-console-consumer.sh --zookeeper localhost:2182 --topic output
Run example application and publish a few words on input topic using Kafka console producer and check the processing result on output topic using Kafka console producer.