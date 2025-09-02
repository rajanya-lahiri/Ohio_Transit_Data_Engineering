{{ config(materialized='view') }}

SELECT
  r.route_long_name,
  COUNT(*) AS trips_count
FROM {{ ref('stg_trips') }} t
JOIN {{ ref('stg_routes') }} r USING (route_id)
GROUP BY r.route_long_name
ORDER BY trips_count ASC
LIMIT 9

-- WITH route_counts AS (
--   SELECT
--     t.route_long_name,
--     COUNT(*) AS trips_count
--   FROM {{ ref('stg_trips') }} t
--   GROUP BY t.route_long_name
-- )
-- SELECT *
-- FROM route_counts
-- ORDER BY trips_count ASC
-- LIMIT 10