-- {{ config(materialized='view') }}

-- select
--   r.route_long_name
-- from {{ ref('stg_trips') }} t
-- join {{ ref('stg_routes') }} r using (route_id)
-- group by r.route_long_name
-- order by count(*) desc
-- limit 1

{{ config(materialized='view') }}

with trips_per_route as (
  select
    route_id,
    count(*) as trips_count
  from {{ ref('stg_trips') }}
  group by route_id
),
winner as (
  select route_id
  from trips_per_route
  order by trips_count desc, route_id
  limit 1
)
select r.route_long_name
from winner w
join {{ ref('stg_routes') }} r using (route_id)
limit 1