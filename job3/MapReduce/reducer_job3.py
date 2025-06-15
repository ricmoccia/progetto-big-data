#!/usr/bin/env python3
import sys

current_key = None
total_price = 0
count = 0
max_hp = 0
best_model = ""

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    key, value = line.split('\t')
    try:
        model, hp, disp, price = value.split(',')
        hp = float(hp)
        price = float(price)
    except:
        continue

    if key != current_key:
        if current_key and count > 0:
            avg_price = round(total_price / count, 2)
            print(f"{current_key}\t{count} auto | Prezzo medio: {avg_price} | Modello top potenza: {best_model} ({max_hp} HP)")
        current_key = key
        total_price = price
        count = 1
        max_hp = hp
        best_model = model
    else:
        total_price += price
        count += 1
        if hp > max_hp:
            max_hp = hp
            best_model = model

# Ultimo gruppo
if current_key and count > 0:
    avg_price = round(total_price / count, 2)
    print(f"{current_key}\t{count} auto | Prezzo medio: {avg_price} | Modello top potenza: {best_model} ({max_hp} HP)")
