-- 1. Creo una tabella Hive per i dati del dataset (se non già fatto)
CREATE EXTERNAL TABLE IF NOT EXISTS used_cars (
  city STRING,
  year INT,
  price DOUBLE,
  daysonmarket INT,
  description STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/used_cars';  -- cambia il path se necessario

-- 2. Creo una tabella temporanea con le fasce di prezzo
CREATE TABLE IF NOT EXISTS car_price_bands AS
SELECT
  city,
  year,
  CASE
    WHEN price < 20000 THEN 'bassa'
    WHEN price >= 20000 AND price <= 50000 THEN 'media'
    ELSE 'alta'
  END AS fascia_prezzo,
  daysonmarket,
  description
FROM used_cars
WHERE price IS NOT NULL AND daysonmarket IS NOT NULL AND year IS NOT NULL;

-- 3. Calcolo numero auto e media giorni sul mercato per fascia/città/anno
CREATE TABLE IF NOT EXISTS stat_fasce AS
SELECT
  city,
  year,
  fascia_prezzo,
  COUNT(*) AS numero_auto,
  AVG(daysonmarket) AS media_daysonmarket
FROM car_price_bands
GROUP BY city, year, fascia_prezzo;

-- 4. Estraggo parole dalle descrizioni e le conteggio per fascia/città/anno
CREATE TABLE IF NOT EXISTS parole_descrizione AS
SELECT
  city,
  year,
  fascia_prezzo,
  word,
  COUNT(*) AS freq
FROM (
  SELECT
    city,
    year,
    fascia_prezzo,
    explode(split(lower(description), '\\s+')) AS word
  FROM car_price_bands
  WHERE description IS NOT NULL
) parole
WHERE length(word) > 2  -- escludo parole troppo corte
GROUP BY city, year, fascia_prezzo, word;

-- 5. Ottengo le 3 parole più frequenti per ogni gruppo (usando funzione RANK o LIMIT + JOIN)
-- Hive non supporta facilmente "TOP N per gruppo" senza subquery complesse, quindi creo una vista per ciascun gruppo
-- (oppure si può fare in Spark SQL)

-- Esempio semplificato con solo la top 3 globale per ciascun gruppo:
CREATE TABLE IF NOT EXISTS top3_words_per_group AS
SELECT city, year, fascia_prezzo, word, freq
FROM (
  SELECT *,
         row_number() OVER (PARTITION BY city, year, fascia_prezzo ORDER BY freq DESC) as rank
  FROM parole_descrizione
) tmp
WHERE rank <= 3;

-- 6. Unisco tutto in una vista finale
CREATE VIEW IF NOT EXISTS report_finale_job2 AS
SELECT
  s.city,
  s.year,
  s.fascia_prezzo,
  s.numero_auto,
  s.media_daysonmarket,
  collect_list(t.word) AS parole_top3
FROM stat_fasce s
LEFT JOIN top3_words_per_group t
  ON s.city = t.city AND s.year = t.year AND s.fascia_prezzo = t.fascia_prezzo
GROUP BY s.city, s.year, s.fascia_prezzo, s.numero_auto, s.media_daysonmarket;
