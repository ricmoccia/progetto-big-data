from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, collect_set, count
import sys

# Controllo argomenti
if len(sys.argv) != 3:
    print("Usage: spark-submit spark_job1_df.py <input_path> <output_path>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

# Inizializza SparkSession
spark = SparkSession.builder \
    .appName("Job1_Make_Model_Stats_DF") \
    .getOrCreate()

# Carica CSV
df = spark.read \
    .option("header", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", True) \
    .csv(input_path)


# Seleziona colonne utili e rimuove righe con null
df_clean = df.select("make_name", "model_name", "price", "year") \
    .where((col("make_name").isNotNull()) & 
           (col("model_name").isNotNull()) & 
           (col("price").isNotNull()) &
           (col("year").isNotNull()))

# Converte tipo colonne numeriche
df_clean = df_clean.withColumn("price", col("price").cast("float"))
df_clean = df_clean.withColumn("year", col("year").cast("int"))

# Aggregazione
agg_df = df_clean.groupBy("make_name", "model_name") \
    .agg(
        count("*").alias("count"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        avg("price").alias("avg_price"),
        collect_set("year").alias("years")
    )

# Funzione robusta di formattazione
def format_row(row):
    def safe_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    min_price = safe_float(row.min_price)
    max_price = safe_float(row.max_price)
    avg_price = safe_float(row.avg_price)
    
    years = sorted([str(y) for y in row.years]) if row.years else []
    years_str = ", ".join(years) if years else "N/D"

    return f"{row.make_name},{row.model_name:<20} {row['count']} auto  Prezzo min: {min_price:.2f}    Prezzo max: {max_price:.2f}    Prezzo medio: {avg_price:.2f}  Anni: {years_str}"

# Applica formattazione e salva output
agg_df.rdd.map(format_row).saveAsTextFile(output_path)

spark.stop()
