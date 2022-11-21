# Twitter-Analysis-With-Spark-Kafka

Start Zookeeper - bin/zookeeper-server-start.sh config/zookeeper.properties

Start Kafka - bin/kafka-server-start.sh config/server.properties

Create topic - bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic dbt

Check topic - bin/kafka-topics.sh --describe --topic dbt --bootstrap-server localhost:9092

Check if topic is receiving - bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic test

bin/spark-submit --master local --class "org.myspark.KafkaStream" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 '/Users/raghavsake/Desktop/DBT Project/consumer.py'
