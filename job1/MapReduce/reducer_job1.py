#!/usr/bin/env python3
import sys
from collections import defaultdict

current_key = None
prices = []
years = set()

def emit_result(make_model, prices, years):
    count = len(prices)
    min_price = min(prices)
    max_price = max(prices)
    avg_price = sum(prices) / count
    year_list = sorted(years)
    print(f"{make_model}\t{count} auto\tPrezzo min: {min_price:.2f}\tPrezzo max: {max_price:.2f}\tPrezzo medio: {avg_price:.2f}\tAnni: {', '.join(year_list)}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, value = line.split('\t')
    try:
        price_str, year = value.split(',')
        price = float(price_str)
    except:
        continue

    if current_key == key:
        prices.append(price)
        years.add(year)
    else:
        if current_key:
            emit_result(current_key, prices, years)
        current_key = key
        prices = [price]
        years = set([year])

# Ultimo gruppo
if current_key:
    emit_result(current_key, prices, years)
