-- {{ config(materialized='view') }}

-- -- Clean + type the raw GTFS calendar_dates table
-- with src as (
--     select * from {{ source('gtfs_raw', 'calendar_dates') }}
-- )

-- select
--     -- service_id is a string in GTFS (can include dots like "107.0.3")
--     cast(service_id as string) as service_id,

--     -- Convert YYYYMMDD integer/string to DATE
--     safe_parse_date('%Y%m%d', cast(date as string)) as date,

--     -- 1 = service added, 2 = service removed
--     cast(exception_type as int64) as exception_type,

--     -- Convenience flag columns
--     case when exception_type = 1 then true end as is_added_service,
--     case when exception_type = 2 then true end as is_removed_service

-- from src

{{ config(materialized='view') }}

with src as (
  select * from {{ source('gtfs_raw', 'calendar_dates') }}
)

select
  cast(service_id as string) as service_id,
  SAFE.PARSE_DATE('%Y%m%d', cast(date as string)) as date,
  cast(exception_type as int64) as exception_type,
  case when exception_type = 1 then true end as is_added_service,
  case when exception_type = 2 then true end as is_removed_service
from src