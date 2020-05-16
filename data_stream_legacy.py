import json
import logging
import os
from uuid import uuid4

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def run_spark_job(sc):
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("file:///tmp/spark")

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

    # # TODO get the right radio code json path
    # radio_code_json_filepath = ""
    # radio_code_df = spark.read.json(radio_code_json_filepath)

    # # clean up your data so that the column names match on radio_code_df and agg_df
    # # we will want to join on the disposition code

    # # TODO rename disposition_code column to disposition
    # radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # # TODO join on disposition column
    # join_query = agg_df.

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SF Crime Stats Analyzer")
    conf.set("spark.streaming.kafka.maxRatePerPartition", 100000)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    logger.warning("Spark context created")

    run_spark_job(sc)
