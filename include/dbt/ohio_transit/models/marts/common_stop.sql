{{ config(materialized='view') }}

with per_stop as (
  select
    st.stop_id,
    count(distinct t.route_id) as routes_served
  from {{ ref('stg_stop_times') }} st
  join {{ ref('stg_trips') }} t using (trip_id)
  where st.stop_id is not null and t.route_id is not null
  group by st.stop_id
),
winner as (
  select stop_id
  from per_stop
  order by routes_served desc, stop_id  -- deterministic tie-break
  limit 1
)
select s.stop_name
from winner w
join {{ ref('stg_stops') }} s using (stop_id)
limit 1