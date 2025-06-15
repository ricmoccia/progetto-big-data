#!/usr/bin/env python3
import sys
import csv
import re

stopwords = {"the", "a", "an", "of", "to", "in", "and", "for", "with", "on", "at", "by"}

reader = csv.reader(sys.stdin)
header = next(reader)

for row in reader:
    try:
        city = row[0]
        days = int(row[1])
        description = row[2].lower()
        price = float(row[7])
        year = row[8]

        # Fascia prezzo
        if price < 20000:
            fascia = "bassa"
        elif price <= 50000:
            fascia = "media"
        else:
            fascia = "alta"

        # Tokenizza descrizione
        parole = re.findall(r'\b\w+\b', description)
        parole = [p for p in parole if p not in stopwords]

        key = f"{city},{year},{fascia}"
        value = f"{days}|" + "|".join(parole)
        print(f"{key}\t{value}")

    except Exception:
        continue
