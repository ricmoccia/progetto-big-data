import pandas as pd

input_file = '../dati_orig/used_cars_data.csv'
columns_needed = [
    'make_name', 'model_name', 'price', 'year',
    'city', 'description', 'daysonmarket',
    'horsepower', 'engine_displacement'
]

chunksize = 100000
cleaned_chunks = []

print("Inizio lettura e pulizia a blocchi...")

for chunk in pd.read_csv(input_file, usecols=columns_needed, low_memory=False, chunksize=chunksize):
    # Dropna
    chunk = chunk.dropna()

    # Pulizia campi numerici
    try:
        chunk = chunk[chunk['price'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]
        chunk['price'] = chunk['price'].astype(float)

        chunk = chunk[chunk['year'].apply(lambda x: str(x).isdigit())]
        chunk['year'] = chunk['year'].astype(int)

        chunk = chunk[chunk['daysonmarket'].apply(lambda x: str(x).isdigit())]
        chunk['daysonmarket'] = chunk['daysonmarket'].astype(int)

        chunk = chunk[chunk['horsepower'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]
        chunk['horsepower'] = chunk['horsepower'].astype(float)

        chunk = chunk[chunk['engine_displacement'].apply(lambda x: str(x).replace('.', '', 1).isdigit())]
        chunk['engine_displacement'] = chunk['engine_displacement'].astype(float)
        
        cleaned_chunks.append(chunk)
        print(f"✔️ Blocco pulito: {len(chunk)} righe")

    except Exception as e:
        print(f"⚠️ Errore nel blocco: {e}")

# Unisci tutti i blocchi
df_clean = pd.concat(cleaned_chunks, ignore_index=True)

# Salva i risultati
df_clean.to_csv('../dati_puliti/cars_clean_base.csv', index=False)
df_clean.sample(10000, random_state=42).to_csv('../dati_puliti/cars_10k.csv', index=False)
df_clean.sample(100000, random_state=42).to_csv('../dati_puliti/cars_100k.csv', index=False)
df_clean.sample(500000, random_state=42).to_csv('../dati_puliti/cars_500k.csv', index=False)
df_clean.to_csv('../dati_puliti/cars_full.csv', index=False)

print("Pulizia completata e file salvati.")
