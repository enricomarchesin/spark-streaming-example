import json
import logging
import os
from uuid import uuid4

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def run_spark_job(sc):
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("file:///tmp/spark")
    sqlc = SQLContext(sc)

    gid = str(uuid4())[:6]
    kafka_topic = 'sf.stats.crimes'
    kafka_stream = KafkaUtils.createDirectStream(
        ssc=ssc,
        topics=[kafka_topic],
        kafkaParams={
            "bootstrap.servers": "localhost:9092",
            "auto.offset.reset": "smallest",
        },
    )

    parsed = kafka_stream.map(lambda v: json.loads(v[1]))
    parsed.count().map(lambda x:'Events in this batch: %s' % x).pprint()

    crime_types_dstream = parsed.map(lambda event: event['original_crime_type_name'])
    crime_types_counts = crime_types_dstream.countByValue()
    # crime_types_counts.pprint()
    crime_types_counts_sorted = crime_types_counts.transform((lambda rdd: rdd.sortBy(lambda r:( -r[1]))))
    crime_types_counts_sorted.pprint(3)

    crime_types_counts_sorted_windowed = crime_types_counts_sorted.window(60, 15)
    crime_types_counts_sorted_windowed.pprint()


    # Get radio code json
    radio_code_json_filename = "radio_code.json"
    radio_code_df = sqlc.read.json(radio_code_json_filename, multiLine=True)

    # clean up data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # rename disposition_code column to disposition
    radio_code_df_renamed = (
        radio_code_df
            .withColumnRenamed("disposition_code", "disposition")
            .withColumnRenamed("description", "disposition_description")
    )
    radio_code_df_renamed.select('*').show(truncate=False)

    # join on disposition column
    # joined_df = parsed.join(radio_code_df_renamed, "disposition")

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SF Crime Stats Analyzer")
    conf.set("spark.streaming.kafka.maxRatePerPartition", 10000)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    logger.warning("Spark context created")

    run_spark_job(sc)
