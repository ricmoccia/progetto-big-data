from pyspark import SparkContext
import sys
import re
from collections import Counter

def parse_line(line):
    try:
        fields = line.strip().split(",", 8)
        if len(fields) != 9:
            return None
        city = fields[0].strip()
        days = int(fields[1].strip())
        description = fields[2].strip().lower()
        price = float(fields[7].strip())
        year = int(fields[8].strip())
        return (city, year, price, days, description)
    except:
        return None

def get_price_range(price):
    if price < 10000:
        return "bassa"
    elif price <= 30000:
        return "media"
    else:
        return "alta"

def tokenize(text):
    return re.findall(r'\b\w+\b', text)

def create_key(record):
    city, year, price, days, desc = record
    price_range = get_price_range(price)
    return ((city, year, price_range), (1, days, tokenize(desc)))

def aggregate_values(a, b):
    return (
        a[0] + b[0],                     # count
        a[1] + b[1],                     # sum days
        a[2] + b[2]                      # concatenated word list
    )

def format_output(record):
    (city, year, price_range), (count, total_days, words) = record
    avg_days = total_days / count if count else 0
    word_counts = Counter(words)
    top_words = ", ".join([word for word, _ in word_counts.most_common(3)])
    return f"{city},{year},{price_range},{count} auto, media giorni: {avg_days:.1f}, top parole: {top_words}"

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: spark-submit spark_job2_bis.py <input> <output>")
        sys.exit(-1)

    sc = SparkContext(appName="Job2BisSparkRDD")

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    lines = sc.textFile(input_path)
    header = lines.first()
    data = lines.filter(lambda row: row != header).map(parse_line).filter(lambda x: x is not None)

    aggregated = (
        data
        .map(create_key)
        .reduceByKey(aggregate_values)
        .map(format_output)
    )

    aggregated.saveAsTextFile(output_path)

    sc.stop()
