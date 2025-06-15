#!/usr/bin/env python3
import sys
from collections import Counter

current_key = None
total_days = 0
count = 0
word_counter = Counter()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    key, value = line.split('\t')
    parts = value.split('|')
    try:
        days = int(parts[0])
        parole = parts[1:]
    except:
        continue

    if key != current_key:
        if current_key:
            media_days = total_days / count if count else 0
            top_words = [w for w, _ in word_counter.most_common(3)]
            print(f"{current_key}\t{count} auto | Giorni medi sul mercato: {round(media_days,1)} | Top 3 parole: {top_words}")
        current_key = key
        total_days = 0
        count = 0
        word_counter = Counter()

    total_days += days
    count += 1
    word_counter.update(parole)

# Output finale
if current_key:
    media_days = total_days / count if count else 0
    top_words = [w for w, _ in word_counter.most_common(3)]
    print(f"{current_key}\t{count} auto | Giorni medi sul mercato: {round(media_days,1)} | Top 3 parole: {top_words}")
