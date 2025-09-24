{{ config(materialized='view') }}

-- Returns only the rows from the most recent batch_id in RAW.RAW_CTGOV_STUDIES
WITH per_batch AS (
  SELECT batch_id, MAX(ingestion_timestamp) AS max_ts
  FROM {{ source('raw', 'ctgov_studies') }}
  GROUP BY batch_id
),
latest AS (
  SELECT batch_id
  FROM per_batch
  QUALIFY ROW_NUMBER() OVER (ORDER BY max_ts DESC) = 1
)
SELECT *
FROM {{ source('raw', 'ctgov_studies') }}
WHERE batch_id = (SELECT batch_id FROM latest)