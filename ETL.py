import time
import findspark

findspark.init()
import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
from time import sleep
from pyspark.sql.types import *

# environment data
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4," \
                             "com.microsoft.azure:spark-mssql-connector:1.0.2 pyspark-shell"


def init_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


# SQL SERVER DATA
server_name = "SQLServerName"
database_name = "DatabaseName"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "TableName"
username = "UserName"
password = "Password"


def add_additional_data():
    stations = spark.read.text('ghcnd-stations.txt').withColumn("StationId", F.expr("substring(value,0,11)")) \
        .withColumn("Latitude", F.expr("substring(value,12,9)").cast(DoubleType())) \
        .withColumn("Longitude", F.expr("substring(value,22,9)").cast(DoubleType())) \
        .withColumn("Elevation", F.expr("substring(value,32,6)").cast(DoubleType())) \
        .drop("value")
    stations = stations.filter((F.col("StationId").startswith("SW")) | (F.col("StationId").startswith("GM")))
    print("\tloading geospatial data 1 into sql")
    try:
        stations.write \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .mode("append") \
            .option("url", url) \
            .option("dbtable", 'StationData') \
            .option("user", username) \
            .option("password", password) \
            .save()
    except ValueError as error:
        print("Connector write failed", error)


def kafka_stream():
    # Define the schema of the data:
    noaa_schema = StructType([StructField('StationId', StringType(), False),
                              StructField('Date', StringType(), False),
                              StructField('Variable', StringType(), False),
                              StructField('Value', IntegerType(), False),
                              StructField('M_Flag', StringType(), True),
                              StructField('Q_Flag', StringType(), True),
                              StructField('S_Flag', StringType(), True),
                              StructField('ObsTime', StringType(), True)])

    string_value_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", "GM, SW") \
        .option("maxOffsetsPerTrigger", 100) \
        .option("startingOffsets", "earliest") \
        .load().selectExpr("CAST(value AS STRING)")
    json_df = string_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
    streaming_df = json_df.select("json.*")
    vars = {'PRCP', 'SNWD', 'TMAX', 'TMIN'}

    def handle_batch(batch_df: pyspark.sql.DataFrame, batch_id):
        start = time.time()
        batch_df = batch_df.filter(batch_df.Q_Flag.isNull()).drop("M_Flag", "Q_Flag", "S_Flag", "ObsTime")
        batch_df = batch_df.withColumn("Year_Rec", F.expr("substring(Date,0,4)").cast(IntegerType())).withColumn(
            "Month_Rec", F.expr("substring(Date,5,2)").cast(IntegerType())).drop("Date")
        batch_df = batch_df.filter(batch_df.Year_Rec >= 1900)
        batch_df = batch_df.filter(
            ((batch_df.Variable == "PRCP") & (batch_df.Value >= 0))
            | (batch_df.Variable == "TMAX")
            | (batch_df.Variable == "TMIN")
            | ((batch_df.Variable == "SNWD") & (batch_df.Value >= 0)))
        ger_swe_df = batch_df.groupBy("StationId", "Year_Rec", "Month_Rec", "Variable") \
            .agg(F.sum(F.col("Value")).alias("batch_sum"), F.count("*").alias("batch_count"))
        ger_swe_df = ger_swe_df.groupby("StationId", "Year_Rec", "Month_Rec").pivot("Variable") \
            .agg(F.first("batch_count").cast(LongType()).alias('batch_count'),
                 F.first("batch_sum").cast(LongType()).alias('batch_sum'))
        cols = {str(col)[:4] for col in ger_swe_df.columns if str(col)[:4] in vars}
        missing = vars - cols
        for var in missing:
            ger_swe_df = ger_swe_df.withColumn(var + '_batch_count', F.lit(None).cast(LongType())).withColumn(
                var + '_batch_sum', F.lit(None).cast(LongType()))
        ger_swe_df = ger_swe_df.select("StationId", "Year_Rec", "Month_Rec", "PRCP_batch_count", 'PRCP_batch_sum',
                                       'TMAX_batch_count', 'TMAX_batch_sum', 'TMIN_batch_count', 'TMIN_batch_sum',
                                       'SNWD_batch_count', 'SNWD_batch_sum') \
            .withColumn("Season", ((F.col("Month_Rec") % 12) / 3 + 1).cast(IntegerType()))
        print("\t writing Ger_Swe batch " + str(batch_id) + " to sql")
        ger_swe_df.withColumn("batchId", F.lit(batch_id)) \
            .write \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .mode("append") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .save()
        print("\t" + str(batch_id) + " took " + str(time.time() - start) + " seconds")

    HOUR = 3600
    query = streaming_df \
        .writeStream \
        .trigger(processingTime='120 seconds') \
        .foreachBatch(handle_batch) \
        .start() \
        .awaitTermination(int(2 * HOUR))


def groupby_batchid():
    ger_swe = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password).load()
    ger_swe = ger_swe.groupBy("StationId", "Year_Rec", "Month_Rec") \
        .agg(F.sum(F.col("PRCP_batch_sum")).alias("PRCP_sum"), F.sum(F.col("PRCP_batch_count")).alias("PRCP_count"),
             F.sum(F.col("TMAX_batch_sum")).alias("TMAX_sum"), F.sum(F.col("TMAX_batch_count")).alias("TMAX_count"),
             F.sum(F.col("TMIN_batch_sum")).alias("TMIN_sum"), F.sum(F.col("TMIN_batch_count")).alias("TMIN_count"),
             F.sum(F.col("SNWD_batch_sum")).alias("SNWD_sum"), F.sum(F.col("SNWD_batch_count")).alias("SNWD_count"),
             F.mean(F.col("Season")).alias("Season")) \
        .drop('PRCP_batch_sum', 'PRCP_batch_count', 'TMAX_batch_sum', 'TMAX_batch_count', 'TMIN_batch_sum',
              'SNWD_batch_count', 'SNWD_batch_sum', 'SNWD_batch_count')
    ger_swe = ger_swe.withColumn("PRCP", F.col('PRCP_sum') / F.col('PRCP_count')) \
        .withColumn("TMAX", (F.col('TMAX_sum') / F.col('TMAX_count')) / 10) \
        .withColumn("TMIN", (F.col('TMIN_sum') / F.col('TMIN_count')) / 10) \
        .withColumn("SNWD", F.col('SNWD_sum') / F.col('SNWD_count')) \
        .drop('PRCP_sum', 'PRCP_count', 'TMAX_sum', 'TMAX_count', 'TMIN_sum', 'TMIN_count', 'SNWD_sum', 'SNWD_count')
    ger_swe.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("append") \
        .option("url", url) \
        .option("dbtable", table_name + "_Stage2") \
        .option("user", username) \
        .option("password", password) \
        .save()


if __name__ == '__main__':
    spark, sc = init_spark('ofek')
    kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
    print("Kafka started streaming:")
    kafka_stream()
    print("\tKafka data streamed")
    print("Grouping and pivoting")
    groupby_batchid()
    print("\tGrouped by batchID, also preformed PIVOT")
    print("Adding Geospatial data to sql")
    add_additional_data()
    print("\tGeospatial data now in sql")
    print("")
    print("done")

# com.microsoft.sqlserver.jdbc.spark
