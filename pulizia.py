import pandas as pd

# Percorso al file originale
input_file = '../dati_orig/used_cars_data.csv'

# Colonne necessarie per tutti e tre i job
columns_needed = [
    'make_name', 'model_name', 'price', 'year',
    'city', 'description', 'daysonmarket',
    'horsepower', 'engine_displacement'
]

# Legge solo le colonne necessarie (risparmia RAM)
df = pd.read_csv(input_file, usecols=columns_needed, low_memory=False)

# Rimuovi righe con valori nulli
df = df.dropna()

# Pulizia dei campi numerici

# Prezzo
df = df[df['price'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]
df['price'] = df['price'].astype(float)

# Anno
df = df[df['year'].apply(lambda x: str(x).isdigit())]
df['year'] = df['year'].astype(int)

# Giorni sul mercato
df = df[df['daysonmarket'].apply(lambda x: str(x).isdigit())]
df['daysonmarket'] = df['daysonmarket'].astype(int)

# Potenza (horsepower)
df = df[df['horsepower'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]
df['horsepower'] = df['horsepower'].astype(float)

# Cilindrata (engine_displacement)
df = df[df['engine_displacement'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]
df['engine_displacement'] = df['engine_displacement'].astype(float)

# Salvataggio dei file puliti
df.to_csv('../dati_puliti/cars_clean_base.csv', index=False)
df.sample(10000, random_state=42).to_csv('../dati_puliti/cars_10k.csv', index=False)
df.sample(100000, random_state=42).to_csv('../dati_puliti/cars_100k.csv', index=False)
df.sample(500000, random_state=42).to_csv('../dati_puliti/cars_500k.csv', index=False)
df.to_csv('../dati_puliti/cars_full.csv', index=False)

print("Pulizia completata. File salvati nella cartella dati_puliti/")
