import pandas as pd

# Percorso corretto al file
df = pd.read_csv("dati_puliti/cars_500k.csv")  # usa / anche su Windows

# Parola da cercare
parola = "AM General"

# Cerca la parola nel dataset e stampa le righe
maschera = df.applymap(lambda x: parola.lower() in str(x).lower() if pd.notnull(x) else False)
righe_trovate = df[maschera.any(axis=1)]

print(righe_trovate)
