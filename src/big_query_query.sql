CREATE OR REPLACE TABLE your_project.dataset.daily_revenue AS
SELECT
  DATE(lpep_pickup_datetime) AS date,
  SUM(fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge) AS total_revenue
FROM
  `your_project.dataset.green_taxi_trips`
WHERE
  EXTRACT(YEAR FROM lpep_pickup_datetime) = 2025
  AND EXTRACT(MONTH FROM lpep_pickup_datetime) IN (1, 2, 3)
GROUP BY
  date
ORDER BY
  date;
