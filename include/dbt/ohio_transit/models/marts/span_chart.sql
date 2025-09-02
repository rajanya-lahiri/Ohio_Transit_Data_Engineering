{{ config(materialized='view') }}

WITH times AS (
  SELECT
    st.trip_id,
    SAFE_CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64)*3600 +
    SAFE_CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64)*60 +
    SAFE_CAST(SPLIT(st.departure_time, ':')[OFFSET(2)] AS INT64) AS departure_secs,
    SAFE_CAST(SPLIT(st.arrival_time, ':')[OFFSET(0)] AS INT64)*3600 +
    SAFE_CAST(SPLIT(st.arrival_time, ':')[OFFSET(1)] AS INT64)*60 +
    SAFE_CAST(SPLIT(st.arrival_time, ':')[OFFSET(2)] AS INT64) AS arrival_secs
  FROM {{ ref('stg_stop_times') }} st
),
trip_bounds AS (
  SELECT
    trip_id,
    MIN(departure_secs) AS first_depart,
    MAX(arrival_secs) AS last_arrive
  FROM times
  GROUP BY trip_id
),
route_bounds AS (
  SELECT
    r.route_long_name,
    MIN(first_depart) AS earliest,
    MAX(last_arrive) AS latest
  FROM trip_bounds tb
  JOIN {{ ref('stg_trips') }} t USING (trip_id)
  JOIN {{ ref('stg_routes') }} r USING (route_id)
  GROUP BY r.route_long_name
)
SELECT
  route_long_name,
  earliest/3600 AS first_hour,
  latest/3600 AS last_hour,
  (latest - earliest)/3600 AS span_hours
FROM route_bounds
ORDER BY span_hours DESC