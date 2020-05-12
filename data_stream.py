import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Schema for incoming resources
# Example:
#     {
#         "crime_id": "182751008",
#         "original_crime_type_name": "Traf Violation Cite",
#         "report_date": "2018-10-02T00:00:00.000",
#         "call_date": "2018-10-02T00:00:00.000",
#         "offense_date": "2018-10-02T00:00:00.000",
#         "call_time": "09:12",
#         "call_date_time": "2018-10-02T09:12:00.000",
#         "disposition": "GOA",
#         "address": "900 Block Of Grove St",
#         "city": "San Francisco",
#         "state": "CA",
#         "agency_id": "1",
#         "address_type": "Premise Address",
#         "common_location": ""
#     },
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True),
])


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sf.stats.crimes") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # # TODO extract the correct column from the kafka input resources
    # # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    service_table.printSchema()

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select("original_crime_type_name", "disposition").distinct()
    # distinct_table = service_table.select("original_crime_type_name", "disposition", "call_date_time").distinct()

    # count the number of original crime type
    agg_df = distinct_table.groupBy("original_crime_type_name").count()

    # agg_df = distinct_table \
    #     .select("original_crime_type_name","call_date_time") \
    #     .withWatermark("call_date_time", "60 minutes") \
    #     .groupBy(psf.window("call_date_time", "10 minutes", "5 minutes"), "original_crime_type_name") \
    #     .count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    # query = agg_df \
    #     .writeStream \
    #     .trigger(processingTime="5 seconds") \
    query = agg_df \
        .writeStream \
        .outputMode('Complete') \
        .format('console') \
        .option("truncate", "false") \
        .start()


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, "disposition")
    join_query.collect().show()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 3000) \
        .appName("SF Crime Stats Analyzer") \
        .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
