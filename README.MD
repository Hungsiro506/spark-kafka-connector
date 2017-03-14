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