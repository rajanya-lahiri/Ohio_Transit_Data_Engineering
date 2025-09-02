{{ config(materialized='view') }}

WITH st AS (
  SELECT
    st.trip_id,
    st.stop_sequence,
    SAFE_CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64)*3600 +
    SAFE_CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64)*60 +
    SAFE_CAST(SPLIT(st.departure_time, ':')[OFFSET(2)] AS INT64) AS departure_secs
  FROM {{ ref('stg_stop_times') }} st
),
first_stops AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT st.*, ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS rn
    FROM st
  )
  WHERE rn = 1
)
SELECT
  CAST(FLOOR(departure_secs/3600) AS INT64) AS hour_of_day,
  CAST(COUNT(*) AS INT64) AS departures
FROM first_stops
GROUP BY hour_of_day
ORDER BY departures DESC
LIMIT 5