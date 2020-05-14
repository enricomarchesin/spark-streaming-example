# 

## How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

I think that, when `.trigger(processingTime=XYZ)` is set on the Structured Stram, the most important parameter is `maxOffsetsPerTrigger`.

Higher numbers on the latter increase throughput, allowing more efficient retrieval of records to process from the source. But this increases also the time it takes to complete the batch, so the `processingTime` interval must be set to a value higher enough to not have the trigger fire before the full batch is processed.

This means that latency for the first messages in the buffer can increase significantly, and that if your load is spiky, your latency can rise unnecessarily.

On the other hand, smaller batches to reduce latency, but there is a risk that the overhead around the processing of the batch (fetching and preparing the data for processing) becomes bigger than the actual processing time.

Another way to control throughput and latency is by using these stream options, I think:

```py
    .option("rowsPerSecond", "5") \
    .option("numPartitions", "1") \
```


## What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Spark provides a huge amount of settings to tweak. The correct recipe strongly depends on the overall architecture of the solution, network latency (between data sources/sinks, Spark driver and Spark executors), and the type and size of data being processed.

In this specific case the dataset is small enough that the deafult size for memory related options (mainly `spark.driver.memory`, `spark.executor.memory`, `spark.executor.pyspark.memory` and `spark.python.worker.memory`) is fine.

We are also running just this task, and in local mode, so there is no need to play with partitions/parallelism (`spark.default.parallelism`, `maxRatePerPartition`, `spark.streaming.kafka.maxRatePerPartition`) and CPU utilisation (`spark.executor.cores`, `spark.cores.max`, `spark.task.cpus`).

