# Streaming Practice

This repository is a practice of streaming data handling using Kafka and Spark Streaming as a project for Udacity Data Streaming Nanodegree.  

This produces a stream stream that sends San Franciso Crime data periodically using Kafka, and consumes the stream data using Kafka and Apache Spark Stream. 


## Required environment

- Spark 2.4.3
- Scala 2.11.x
- Java 1.8.x
- Kafka build with Scala 2.11.x
- Python 3.6.x or 3.7.x
- Anaconda 

## Steps to run 

0.  Install all of the requirements above

1.  Install required packages by 
```
conda install -r requirements.txt
conda install pyspark
pip install kafka-python
 ```

2. Download the data ('police-department-calls-for-service.json') and place on the same folder of `kafka_server.py'. This data comes from Kaggle (https://www.kaggle.com/c/sf-crime/data)


3. Run Zookeeper server. In the folder you have installed kafka, run following
```
./bin/zookeeper-server-start.sh config/zookeeper.properties
```

4. Run Kafka server. In the folder you have installed kafka, run following

```
./bin/kafka-server-start.sh config/server.properties

```
5. Run Kafka Producer server
```
python kafka_server.py

```

6. Find topics . In the folder you have installed kafka, run following

```
./bin/kafka-topics.sh --list --zookeeper localhost:2181
```

7. Consume the stream data.  run following (in case of "new_topic" topic)

```
python consumer_server.py

```
you will find the data like below start flowing (1 data every second)

![data](kafkaConsumerConsole.png)



8. Consume using Spark Streaming by following code 

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 --master local[*] data_stream.py
```

You will see aggregated data like below

![data](Consumer_spark_console.png)


9. Monitroing Spark UI is at `http://10.0.2.15:4040`  on localhost




QUESTIONS:

1.How did changing values on the SparkSession property parameters affect the throughput and      latency of the data?

Ans. Throughput and latency was either increasing or decreasing using the parameters.

a.processedRowsPerSecond

b.maxOffsetPerTrigger

c.maxRatePerPartition

2.What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Ans. The changes in these values made a difference in processing time.Effective to great extent. 

spark.sql.shuffle.partitions                10

spark.streaming.kafka.maxRatePerPartition   10

spark.default.parallelism                   10000
