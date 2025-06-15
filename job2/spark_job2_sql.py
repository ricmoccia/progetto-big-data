#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, countDistinct, count, avg, explode, split, lower, regexp_extract, row_number, collect_list, concat_ws
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
from pyspark.sql.window import Window
import argparse

# Parsing argomenti
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path to input file")
parser.add_argument("-output", type=str, help="Path to output folder")
args = parser.parse_args()

# Spark session
spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("spark-sql#job-2") \
    .getOrCreate()

# Schema
schema = StructType([
    StructField("city", StringType(), True),
    StructField("daysonmarket", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("engine_displacement", DoubleType(), True),
    StructField("horsepower", DoubleType(), True),
    StructField("make_name", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("year", IntegerType(), True)
])

# Carica CSV
df = spark.read.csv(args.input, schema=schema, header=True)

# Fasce di prezzo
df = df.withColumn(
    "price_category",
    when(col("price") > 50000, "high")
    .when((col("price") >= 20000) & (col("price") <= 50000), "medium")
    .when(col("price") < 20000, "low")
    .otherwise(None)
)

# Statistiche per gruppo
agg_stats = df.groupBy("city", "year", "price_category").agg(
    countDistinct("model_name").alias("num_models"),
    count("*").alias("num_cars"),
    avg("daysonmarket").alias("avg_daysonmarket")
)

# ===============================
# Top 3 parole più frequenti
# ===============================

# Pulizia parole da descrizione
words = df.select(
    "city", "year", "price_category",
    explode(split(lower(col("description")), r"\W+")).alias("word")
).filter(col("word") != "")

# Conta parole
word_counts = words.groupBy("city", "year", "price_category", "word") \
    .count()

# Classifica parole per gruppo
windowSpec = Window.partitionBy("city", "year", "price_category").orderBy(col("count").desc())
top_words = word_counts.withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") <= 3)

# Aggrega le top 3 parole in una lista
top_words_agg = top_words.groupBy("city", "year", "price_category") \
    .agg(concat_ws(", ", collect_list("word")).alias("top_words"))

# ===============================
# Join e scrittura finale
# ===============================

# Unisci con le statistiche
final_result = agg_stats.join(top_words_agg, on=["city", "year", "price_category"], how="left")

# Scrivi CSV
final_result.write \
    .option("header", True) \
    .mode("overwrite") \
    .csv(args.output)

# Mostra output di esempio
final_result.show(10, truncate=False)

# Ferma Spark
spark.stop()
