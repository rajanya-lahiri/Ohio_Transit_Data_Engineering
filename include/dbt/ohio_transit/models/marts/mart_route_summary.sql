{{ config(materialized='view') }}

-- select
--   r.route_id,
--   r.route_short_name,
--   count(distinct t.trip_id)         as trips,
--   count(distinct t.trip_headsign)   as unique_headsings,
--   count(distinct t.direction_id)    as directions_present
-- from {{ ref('stg_routes') }} r
-- left join {{ ref('stg_trips') }} t using (route_id)
-- group by 1, 2
-- order by trips desc


select
  r.route_id,
  sc.service_date,
  count(distinct t.trip_id) as trips
from {{ ref('stg_routes') }} r
join {{ ref('stg_trips') }} t using (route_id)
join {{ ref('service_calendar') }} sc using (service_id)
group by 1,2
order by sc.service_date, trips desc