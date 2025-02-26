CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp_coco_kestra/yellow_tripdata_*.parquet']
);

CREATE OR REPLACE TABLE `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_regular` AS
SELECT * FROM `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_external`;

#1
SELECT COUNT(*) FROM `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_regular`;

#2
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocationids_external FROM `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_external`;

SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocationids_regular FROM `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_regular`;

#3
SELECT PULocationID FROM `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_regular`;

SELECT PULocationID, DOLocationID FROM `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_regular`;

#4
SELECT COUNT(*) FROM `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_external` WHERE fare_amount = 0;

#5
CREATE OR REPLACE TABLE `kestra-sandbox-450921.de_zoomcamp.optimized_taxi_data`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_regular`;

#6
SELECT DISTINCT VendorID
FROM `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_regular`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `kestra-sandbox-450921.de_zoomcamp.optimized_taxi_data`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

#9
SELECT count(*) FROM `kestra-sandbox-450921.de_zoomcamp.yellow_taxi_regular`;