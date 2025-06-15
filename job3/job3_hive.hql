-- 1. Creo tabella di input (solo i campi necessari)
CREATE EXTERNAL TABLE IF NOT EXISTS used_cars (
  model_name STRING,
  horsepower DOUBLE,
  engine_displacement DOUBLE,
  price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/used_cars';

-- 2. Filtra righe valide ed elimina outlier/nulli
CREATE TABLE IF NOT EXISTS filtered_cars AS
SELECT *
FROM used_cars
WHERE horsepower IS NOT NULL AND engine_displacement IS NOT NULL AND price IS NOT NULL;

-- 3. Creo "bucket" (gruppi) usando approssimazioni al 10%
CREATE TABLE IF NOT EXISTS grouped_cars AS
SELECT *,
       floor(horsepower / (horsepower * 0.1)) AS hp_group,
       floor(engine_displacement / (engine_displacement * 0.1)) AS ed_group
FROM filtered_cars;

-- 4. Raggruppo per bucket (hp_group, ed_group)
CREATE TABLE IF NOT EXISTS job3_results AS
SELECT
  hp_group,
  ed_group,
  COUNT(*) AS num_modelli,
  AVG(price) AS prezzo_medio,
  MAX(horsepower) AS max_horsepower
FROM grouped_cars
GROUP BY hp_group, ed_group;

-- 5. Trova il modello con potenza massima per ciascun gruppo
CREATE TABLE IF NOT EXISTS modello_con_max_hp AS
SELECT gc.hp_group, gc.ed_group, gc.model_name, gc.horsepower
FROM grouped_cars gc
JOIN (
  SELECT hp_group, ed_group, MAX(horsepower) AS max_hp
  FROM grouped_cars
  GROUP BY hp_group, ed_group
) max_hp_grp
ON gc.hp_group = max_hp_grp.hp_group AND gc.ed_group = max_hp_grp.ed_group AND gc.horsepower = max_hp_grp.max_hp;

-- 6. Join finale: statistiche + modello più potente
CREATE VIEW IF NOT EXISTS report_finale_job3 AS
SELECT
  r.hp_group,
  r.ed_group,
  r.num_modelli,
  r.prezzo_medio,
  m.model_name AS modello_piu_potente,
  r.max_horsepower
FROM job3_results r
LEFT JOIN modello_con_max_hp m
  ON r.hp_group = m.hp_group AND r.ed_group = m.ed_group;
