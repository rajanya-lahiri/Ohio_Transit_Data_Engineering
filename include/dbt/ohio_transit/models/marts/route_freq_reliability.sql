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
),
gaps AS (
  SELECT
    t.route_id,
    r.route_long_name,
    (departure_secs - LAG(departure_secs) OVER (PARTITION BY t.route_id ORDER BY departure_secs))/60.0 AS gap_min
  FROM first_stops fs
  JOIN {{ ref('stg_trips') }}  t USING (trip_id)
  JOIN {{ ref('stg_routes') }} r USING (route_id)
)
SELECT
  route_long_name,
  APPROX_QUANTILES(gap_min, 100)[OFFSET(50)] AS median_headway_min,
  APPROX_QUANTILES(gap_min, 100)[OFFSET(90)] AS p90_headway_min
FROM gaps
WHERE gap_min IS NOT NULL AND gap_min <= 360  -- drop overnight mega-gaps (>6h)
GROUP BY route_long_name
ORDER BY p90_headway_min DESC
LIMIT 5