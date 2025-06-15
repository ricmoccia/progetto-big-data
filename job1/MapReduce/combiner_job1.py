#!/usr/bin/env python3
import sys

from collections import defaultdict

current_key = None
count = 0
sum_price = 0.0
min_price = float('inf')
max_price = float('-inf')
years = set()

def emit_result():
    years_str = ",".join(sorted(years))
    print(f"{current_key}\t{count},{sum_price},{min_price},{max_price},{years_str}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    key, value = line.split("\t")
    parts = value.split(",")
    if len(parts) < 5:
        continue

    try:
        c = int(parts[0])
        p_sum = float(parts[1])
        p_min = float(parts[2])
        p_max = float(parts[3])
        year = parts[4]
    except:
        continue

    if key != current_key:
        if current_key:
            emit_result()
        current_key = key
        count = 0
        sum_price = 0.0
        min_price = float('inf')
        max_price = float('-inf')
        years = set()

    count += c
    sum_price += p_sum
    min_price = min(min_price, p_min)
    max_price = max(max_price, p_max)
    years.add(year)

if current_key:
    emit_result()
