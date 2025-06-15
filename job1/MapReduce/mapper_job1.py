#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

try:
    header = next(reader)  # Salta l'intestazione
except StopIteration:
    sys.exit(0)  # File vuoto, niente da fare

for row in reader:
    if len(row) < 9:
        continue  # Ignora righe incomplete

    try:
        make = row[5].strip()
        model = row[6].strip()
        price = float(row[7])
        year = row[8].strip()

        if make and model and year:
            key = f"{make},{model}"
            value = f"{price},{year}"
            print(f"{key}\t{value}")
    except:
        continue  # Ignora errori di conversione
