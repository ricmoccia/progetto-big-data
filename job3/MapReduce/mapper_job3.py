#!/usr/bin/env python3
import sys
import csv
import math

reader = csv.reader(sys.stdin)
header = next(reader)

for row in reader:
    try:
        model = row[6]
        hp = float(row[4])
        disp = float(row[3])
        price = float(row[7])

        # Raggruppamento per "range del 10%"
        hp_bucket = round(hp / 10)
        disp_bucket = round(disp / 0.1)

        key = f"{hp_bucket},{disp_bucket}"
        value = f"{model},{hp},{disp},{price}"
        print(f"{key}\t{value}")

    except:
        continue
