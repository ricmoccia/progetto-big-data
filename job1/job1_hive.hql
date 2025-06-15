CREATE TABLE auto_raw (
    make_name STRING,
    model_name STRING,
    price FLOAT,
    year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/riccardo/progettoBigData/dataset/cars_100k_clean.csv'
OVERWRITE INTO TABLE auto_raw;

CREATE TABLE job1_result AS
SELECT
  make_name,
  model_name,
  COUNT(*) AS num_auto,
  MIN(price) AS prezzo_min,
  MAX(price) AS prezzo_max,
  ROUND(AVG(price), 2) AS prezzo_medio,
  COLLECT_SET(year) AS anni_presenti
FROM auto_raw
GROUP BY make_name, model_name
ORDER BY make_name, model_name;

SELECT * FROM job1_result LIMIT 10;

DROP TABLE auto_raw;
DROP TABLE job1_result;
