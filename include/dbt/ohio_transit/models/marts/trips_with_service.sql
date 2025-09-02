{{ config(materialized='view') }}

select
  t.trip_id,
  t.route_id,
  t.service_id,
  sc.service_date,
  t.direction_id,
  t.shape_id
from {{ ref('stg_trips') }} t
join {{ ref('service_calendar') }} sc
  using (service_id)