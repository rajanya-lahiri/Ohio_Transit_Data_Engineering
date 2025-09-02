{{ config(materialized='view') }}

with st as (
  select
    st.trip_id,
    -- times beyond 24:00:00 handled via split-to-seconds
    safe_cast(split(st.arrival_time, ':')[offset(0)] as int64)*3600 +
    safe_cast(split(st.arrival_time, ':')[offset(1)] as int64)*60 +
    safe_cast(split(st.arrival_time, ':')[offset(2)] as int64) as arrival_secs,
    safe_cast(split(st.departure_time, ':')[offset(0)] as int64)*3600 +
    safe_cast(split(st.departure_time, ':')[offset(1)] as int64)*60 +
    safe_cast(split(st.departure_time, ':')[offset(2)] as int64) as departure_secs,
    st.stop_sequence
  from {{ ref('stg_stop_times') }} st
),
per_trip as (
  select
    trip_id,
    min(departure_secs) as first_depart,
    max(arrival_secs)   as last_arrive
  from st
  group by trip_id
),
longest as (
  select
    trip_id,
    (last_arrive - first_depart) as trip_span_secs
  from per_trip
  where first_depart is not null and last_arrive is not null and last_arrive >= first_depart
  order by trip_span_secs desc
  limit 1
)
select
  r.route_long_name
from longest l
join {{ ref('stg_trips') }} t using (trip_id)
join {{ ref('stg_routes') }} r using (route_id)
limit 1

-- {{ config(materialized='view') }}

-- with stops_per_trip as (
--   select
--     trip_id,
--     count(distinct stop_id) as stop_count
--   from {{ ref('stg_stop_times') }}
--   group by trip_id
--   order by stop_count desc
--   limit 1
-- )
-- select
--   r.route_long_name
-- from stops_per_trip spt
-- join {{ ref('stg_trips') }} t using (trip_id)
-- join {{ ref('stg_routes') }} r using (route_id)
-- limit 1