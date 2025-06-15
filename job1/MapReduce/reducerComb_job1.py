#!/usr/bin/env python3
import sys

current_key = None
count = 0
sum_price = 0.0
min_price = float('inf')
max_price = float('-inf')
years = set()

def emit_result():
    if count == 0:
        return
    avg_price = sum_price / count
    years_str = ",".join(sorted(years))
    print(f"{current_key}\t{count},{min_price:.2f},{max_price:.2f},{avg_price:.2f},{years_str}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, value = line.split("\t")
        parts = value.split(",")

        c = int(parts[0])
        p_sum = float(parts[1])
        p_min = float(parts[2])
        p_max = float(parts[3])
        anni = parts[4:]  # Tutti gli anni (da indice 4 in poi)
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
    years.update(anni)

# Emit dell'ultima chiave
if current_key:
    emit_result()
