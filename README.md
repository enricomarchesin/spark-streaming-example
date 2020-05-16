# Udacity Data Streaming Nanodegree - S.F. Crime Stats


## How to use


### 1. One time setup

As always, make sure you are in a dedicated virtualenv (I manage them using `virtualenvwrapper`), after which the dependencis in `requirements.txt` can be installed:

```bash
mkvirtualenv udacity-spark-project
pip install -r requirements.txt
```


### 2. Start Kafka cluster

Then the first thing to do is standing up a Kafka cluster to stream data to and from. This can be easily done using the provided docker-compose.yml config:

```bash
docker-compose up -d
```

After 20-40 seconds (depending on how powerful your development machine is), the cluster should be up. 

You can check its state using the following commands:

```bash
docker-compose ps
docker-compose logs
```


### 3. Get sample data into local Kafka cluster

The first thing we need to do is push the sample data to a Kafka topic (`sf.stats.crimes`):

```bash
python kafka_server.py
```

One dot will be printed every 100 call events published, and each line will contain 100 dots. There are 199999 calls total in the JSON file: with 100*100 messages per line, a total of 20 lines should be printed.


### 4a. Consume using Spark Structured Streaming

Just run:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 data_stream.py 2>/dev/null
```

You will find the Spark UI at: http://localhost:4040/


### 4b. Consume using Spark Streaming (DStreams)

Use this command:

```bash
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5 data_stream_legacy.py
```

The Spark Streaming UI will be available at: http://localhost:4040/streaming/


## Questions about Structured Streaming approach


### Q1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

When `.trigger(processingTime=XYZ)` is set on the Structured Stram, the most important parameter is `maxOffsetsPerTrigger`.

Higher numbers on the latter increase throughput, allowing more efficient retrieval of records to process from the source. But this increases also the time it takes to complete the batch, so the `processingTime` interval must be set to a value higher enough to avoid that the trigger fires before the full batch is processed.

This means that latency for the first messages in the buffer can increase significantly. When your load is spiky this can be more noticeable, and processing latency can rise unnecessarily since no further processing will happen until `processingTime` interval has passed.

On the other hand, smaller batches will reduce latency, but there is a risk that the overhead around the processing of the single batch (fetching and preparing the data for processing) becomes bigger than the actual processing time.

Another way to control throughput and latency is using these options on the stream itself:

```py
    .option("rowsPerSecond", "5") \
    .option("numPartitions", "1") \
```


### Q2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Spark provides a huge amount of settings to tweak. The correct recipe strongly depends on the overall architecture of the solution, network latency (between data sources/sinks, Spark driver and Spark executors), and the type and size of data being processed.

In this specific case the dataset is small enough that the default size for memory related options (mainly `spark.driver.memory`, `spark.executor.memory`, `spark.executor.pyspark.memory` and `spark.python.worker.memory`) is fine.

We are also running just this task, and in local mode, so there is no need to play with partitions/parallelism (`spark.default.parallelism`, `maxRatePerPartition`, `spark.streaming.kafka.maxRatePerPartition`) and CPU utilisation (`spark.executor.cores`, `spark.cores.max`, `spark.task.cpus`).

If I had to pick two that important in most cases, I'd choose: `spark.default.parallelism`, `maxRatePerPartition`.
