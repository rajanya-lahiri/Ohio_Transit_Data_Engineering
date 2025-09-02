-- Avg number of stops on a single trip (across all trips in the feed)
with per_trip as (
  select
    st.trip_id,
    count(distinct st.stop_sequence) as stops_per_trip
  from {{ ref('stg_stop_times') }} st
  -- (optional but harmless) ensure trip_id exists in trips
  join {{ ref('stg_trips') }} t using (trip_id)
  group by st.trip_id
)
select round(avg(stops_per_trip), 2) as avg_stops_per_trip
from per_trip