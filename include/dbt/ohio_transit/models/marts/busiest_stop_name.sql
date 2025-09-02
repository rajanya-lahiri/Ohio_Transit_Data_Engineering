-- {{ config(materialized='view') }}

-- with busiest as (
--     select
--         stop_name,
--         arrivals
--     from {{ ref('mart_busiest_stops') }}
--     order by arrivals desc
--     limit 1
-- )

-- select * from busiest


{{ config(materialized='view') }}

SELECT
  s.stop_name,
  COUNT(*) AS arrivals
FROM {{ ref('stg_stop_times') }} st
JOIN {{ ref('stg_stops') }} s USING (stop_id)
GROUP BY s.stop_name
ORDER BY arrivals DESC
LIMIT 1