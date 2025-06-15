from pyspark import SparkContext
import sys
import shutil
import os
import time

# Leggi i percorsi input/output da sys.argv
input_path = sys.argv[1]
output_path = sys.argv[2]

# Se la cartella di output esiste, rimuovila
if os.path.exists(output_path.replace("file://", "")):
    shutil.rmtree(output_path.replace("file://", ""))

sc = SparkContext(appName="Job2")

start_time = time.time()

# Caricamento dati
lines = sc.textFile(input_path)
header = lines.first()
data = lines.filter(lambda line: line != header)

# Estrai coppie (city, parola) da descrizione
def estrai_descrizione(line):
    try:
        parts = list(csv.reader([line]))[0]
        city = parts[0]
        description = parts[2].replace(",", " ").replace("\"", "").strip()
        parole = description.split()
        return [(city + "," + parola.strip(), 1) for parola in parole if parola.strip()]
    except Exception:
        return []

import csv
city_parola = data.flatMap(estrai_descrizione)
conteggi = city_parola.reduceByKey(lambda a, b: a + b)

# Salvataggio
conteggi.map(lambda x: f"{x[0]}  {x[1]}").saveAsTextFile(output_path)

end_time = time.time()
print(f"\nTempo di esecuzione: {end_time - start_time:.2f} secondi")
sc.stop()
