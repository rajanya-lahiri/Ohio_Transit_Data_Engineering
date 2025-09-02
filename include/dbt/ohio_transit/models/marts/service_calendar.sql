{{ config(materialized='view') }}

with base as (
  select
    service_id,
    start_date,
    end_date,
    monday, tuesday, wednesday, thursday, friday, saturday, sunday
  from {{ ref('stg_calendar') }}
),
expanded as (
  -- One row per service_id per date in range
  select
    b.service_id,
    d as service_date,
    case extract(dayofweek from d)
      when 1 then b.sunday
      when 2 then b.monday
      when 3 then b.tuesday
      when 4 then b.wednesday
      when 5 then b.thursday
      when 6 then b.friday
      when 7 then b.saturday
    end as weekday_active
  from base b,
  unnest(generate_date_array(b.start_date, b.end_date)) as d
),
exceptions as (
  select
    service_id,
    date as service_date,
    exception_type  -- 1 = add service, 2 = remove service
  from {{ ref('stg_calendar_dates') }}
),
merged as (
  select
    e.service_id,
    e.service_date,
    case
      when ex.exception_type = 1 then 1
      when ex.exception_type = 2 then 0
      else e.weekday_active
    end as is_active
  from expanded e
  left join exceptions ex
    on e.service_id = ex.service_id
   and e.service_date = ex.service_date
)
select
  service_id,
  service_date
from merged
where is_active = 1