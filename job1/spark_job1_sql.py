#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, round as spark_round
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path to input CSV file")
parser.add_argument("-output", type=str, help="Path to output folder")
args = parser.parse_args()

spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("spark-sql#job-1") \
    .getOrCreate()

schema = StructType([
    StructField(name="city", dataType=StringType(), nullable=True),
    StructField(name="daysonmarket", dataType=IntegerType(), nullable=True),
    StructField(name="description", dataType=StringType(), nullable=True),
    StructField(name="engine_displacement", dataType=DoubleType(), nullable=True),
    StructField(name="horsepower", dataType=DoubleType(), nullable=True),
    StructField(name="make_name", dataType=StringType(), nullable=True),
    StructField(name="model_name", dataType=StringType(), nullable=True),
    StructField(name="price", dataType=DoubleType(), nullable=True),
    StructField(name="year", dataType=IntegerType(), nullable=True)
])

# Lettura CSV con gestione multilinea e virgolette
df = spark.read \
    .option("header", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .schema(schema) \
    .csv(args.input)

df.createOrReplaceTempView("dataset")

model_stats_query = """
SELECT 
    make_name,
    model_name,
    COUNT(*) as num_cars,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price,
    COLLECT_SET(year) as years_list
FROM dataset
GROUP BY make_name, model_name
"""

model_stats = spark.sql(model_stats_query)

model_stats = model_stats \
    .withColumn("avg_price", spark_round(col("avg_price"), 2)) \
    .withColumn("years_list", concat_ws(",", col("years_list")))

# Scrivi output come CSV (disabilita se vuoi solo vedere a schermo)
model_stats.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(args.output)

# Stampa in console le prime 10 righe
model_stats.show(10, truncate=False)

spark.stop()
