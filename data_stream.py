import logging
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Schema for incoming resources
calls_schema = StructType([
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

    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = (
        spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "sf.stats.crimes")
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 10000)
            .option("stopGracefullyOnShutdown", "true")
            .load()
    )

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), calls_schema).alias("DF"))\
        .select("DF.*")

    service_table.printSchema()

    # Select original_crime_type_name and disposition and count the number of original crime type
    distinct_table = service_table.select("original_crime_type_name", "disposition").distinct()
    agg_df = distinct_table.groupBy("original_crime_type_name", "disposition").count()

    # distinct_table = service_table.select("original_crime_type_name", "disposition", "call_date_time").distinct()
    # agg_df = distinct_table \
    #     .select("original_crime_type_name", "disposition", "call_date_time") \
    #     .withWatermark("call_date_time", "60 minutes") \
    #     .groupBy(psf.window("call_date_time", "10 minutes", "5 minutes"), "original_crime_type_name", "disposition") \
    #     .count()

    # Attach a ProgressReporter
    query1 = (
        agg_df
            .writeStream
            .outputMode('Complete')
            .format('console')
            .option("truncate", "false")
            .start()
    )

    # Get radio code json
    radio_code_json_filename = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filename, multiLine=True)

    # clean up data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # rename disposition_code column to disposition
    radio_code_df_renamed = (
        radio_code_df
            .withColumnRenamed("disposition_code", "disposition")
            .withColumnRenamed("description", "disposition_description")
    )
    radio_code_df_renamed.printSchema()

    # join on disposition column
    join_query = (
        distinct_table
            .join(radio_code_df_renamed, "disposition")
            .groupBy("original_crime_type_name", "disposition_description").count()
    )

    # Attach a ProgressReporter
    query2 = (
        join_query
            .writeStream
            .outputMode('Complete')
            .format('console')
            .option("truncate", "false")
            .start()
    )

    query2.awaitTermination()
    query1.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode, using all available CPUs
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 4040) \
        .appName("SF Crime Stats Analyzer") \
        .getOrCreate()
    # spark.conf.set("spark.sql.shuffle.partitions", "8")

    logger.info("Spark started")
    run_spark_job(spark)

    spark.stop()
