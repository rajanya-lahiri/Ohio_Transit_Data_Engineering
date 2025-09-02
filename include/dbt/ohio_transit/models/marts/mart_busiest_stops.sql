{{ config(materialized='view') }}

select
  s.stop_id,
  s.stop_name,
  count(*) as arrivals
from {{ ref('stg_stop_times') }} st
join {{ ref('stg_stops') }} s using (stop_id)
group by 1, 2
order by arrivals desc