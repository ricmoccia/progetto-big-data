from pyspark.sql import SparkSession
import sys
import csv
from io import StringIO

def parse_line(line):
    reader = csv.reader(StringIO(line))
    return next(reader)

def safe_float(x):
    try:
        return float(x)
    except:
        return None

def main(input_path, output_path):
    spark = SparkSession.builder.appName("Job1_Make_Model_Stats").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile(input_path)
    header = rdd.first()
    data = rdd.filter(lambda row: row != header).map(parse_line)

    make_idx = 5
    model_idx = 6
    price_idx = 7
    year_idx = 8


    def map_model(row):
        try:
            make = row[make_idx]
            model = row[model_idx]
            year = row[year_idx]
            price = safe_float(row[price_idx])
            if make and model and price is not None and year.isdigit():
                return ((make, model), (1, price, price, price, [year]))
        except:
            return None
        return None

    print("Numero righe totali:", data.count())
    mapped = data.map(map_model)
    print("Righe valide:", mapped.filter(lambda x: x is not None).count())

    filtered = mapped.filter(lambda x: x is not None)


    def reduce_models(a, b):
        count = a[0] + b[0]
        min_p = min(a[1], b[1])
        max_p = max(a[2], b[2])
        sum_p = a[3] + b[3]
        years = list(set(a[4] + b[4]))
        return (count, min_p, max_p, sum_p, years)

    model_stats = filtered.reduceByKey(reduce_models)

    result = model_stats.map(lambda kv: (
        kv[0][0],  # make
        (kv[0][1], kv[1][0], kv[1][1], kv[1][2], round(kv[1][3]/kv[1][0], 2), sorted(kv[1][4]))
    )).groupByKey().mapValues(list)

    result.map(lambda x: str(x)).saveAsTextFile(output_path)
    spark.stop()

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)

