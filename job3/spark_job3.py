from pyspark import SparkContext
import sys

if len(sys.argv) != 3:
    print("Usage: spark_job3.py <input_file> <output_folder>")
    sys.exit(-1)

input_path = sys.argv[1]
output_path = sys.argv[2]

sc = SparkContext(appName="SparkJob3_AutoSimili")

def parse_line(line):
    try:
        fields = line.split(",")
        horsepower = float(fields[4])              # horsepower
        engine_displacement = float(fields[3])     # engine_displacement
        price = float(fields[7])                   # price
        model_name = fields[6]                     # model_name
        return (horsepower, engine_displacement, price, model_name)
    except:
        return None

def group_key(hp, disp):
    return (round(hp, -1), round(disp, 1))  # es. (200, 3.2)

lines = sc.textFile(input_path)
header = lines.first()
data = lines.filter(lambda l: l != header).map(parse_line).filter(lambda x: x is not None)

grouped = data.map(lambda x: (group_key(x[0], x[1]), (x[0], x[1], x[2], x[3]))) \
              .groupByKey()

def compute_group_info(group):
    values = list(group[1])
    if not values:
        return None
    avg_price = sum(v[2] for v in values) / len(values)
    max_hp_car = max(values, key=lambda v: v[0])  # auto con HP massimo
    return f"Gruppo {group[0]} -> Prezzo medio: {avg_price:.2f}, Modello con max HP: {max_hp_car[3]} ({max_hp_car[0]} hp)"

results = grouped.map(compute_group_info).filter(lambda x: x is not None)

results.saveAsTextFile(output_path)

sc.stop()
