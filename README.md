Streaming Practice
This repository is a practice of streaming data handling using Kafka and Spark Streaming as a project for Udacity Data Streaming Nanodegree.

This produces a stream stream that sends San Franciso Crime data periodically using Kafka, and consumes the stream data using Kafka and Apache Spark Stream.

Required environment
Spark 2.4.3
Scala 2.11.x
Java 1.8.x
Kafka build with Scala 2.11.x
Python 3.6.x or 3.7.x
Anaconda
Steps to run
Install all of the requirements above

Install required packages by

conda install -r requirements.txt
conda install pyspark
pip install kafka-python
Download the data ('police-department-calls-for-service.json') and place on the same folder of `kafka_server.py'. This data comes from Kaggle (https://www.kaggle.com/c/sf-crime/data)

Run Zookeeper server. In the folder you have installed kafka, run following

./bin/zookeeper-server-start.sh config/zookeeper.properties
Run Kafka server. In the folder you have installed kafka, run following
./bin/kafka-server-start.sh config/server.properties

Run Kafka Producer server
python kafka_server.py

Find topics . In the folder you have installed kafka, run following
./bin/kafka-topics.sh --list --zookeeper localhost:2181
Consume the stream data. run following (in case of "new_topic" topic)
python consumer_server.py

you will find the data like below start flowing (1 data every second)

/Users/kuviera/Desktop/TECHNOLOGY/Udacity/Projects/kafka_spark/sf-crime-data-project-files/kafkaConsumerConsole.png

Consume using Spark Streaming by following code
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 --master local[*] data_stream.py
You will see aggregated data like below

/Users/kuviera/Desktop/TECHNOLOGY/Udacity/Projects/kafka_spark/sf-crime-data-project-files/Consumer_spark_console.png
data

Monitroing Spark UI is at http://10.0.2.15:4040 on localhost
