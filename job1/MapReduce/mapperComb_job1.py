#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

try:
    header = next(reader)  # Salta intestazione
except StopIteration:
    sys.exit(0)

for row in reader:
    if len(row) < 9:
        continue  # Righe incomplete

    try:
        make = row[5].strip()
        model = row[6].strip()
        price = float(row[7])
        year = row[8].strip()

        if make and model and year:
            key = f"{make},{model}"
            # Output: count=1, sum=price, min=price, max=price, year
            value = f"1,{price},{price},{price},{year}"
            print(f"{key}\t{value}")
    except:
        continue
