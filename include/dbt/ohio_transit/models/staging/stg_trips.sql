{{ config(materialized='view') }}

-- Clean + type the raw GTFS trips table
with src as (
  select * from {{ source('gtfs_raw', 'trips') }}
)

select
  -- Ids as STRING for consistent joins across the project
  cast(route_id   as string) as route_id,
  cast(service_id as string) as service_id,
  cast(trip_id    as string) as trip_id,

  -- Free-text fields: trim spaces, '' -> NULL
  nullif(trim(cast(trip_headsign   as string)), '') as trip_headsign,
  nullif(trim(cast(trip_short_name as string)), '') as trip_short_name,

  -- GTFS numeric flags / ids: normalize to INT64 (blank -> NULL)
  cast(nullif(cast(direction_id          as string), '') as int64) as direction_id,          -- 0 = outbound, 1 = inbound (convention varies)
  cast(nullif(cast(block_id              as string), '') as int64) as block_id,
  cast(nullif(cast(shape_id              as string), '') as int64) as shape_id,
  cast(nullif(cast(wheelchair_accessible as string), '') as int64) as wheelchair_accessible, -- 0/1/2 per GTFS
  cast(nullif(cast(bikes_allowed         as string), '') as int64) as bikes_allowed          -- 0/1/2 per GTFS

  -- You can add convenience labels here if you want, e.g. direction_name:
  -- , case cast(nullif(cast(direction_id as string), '') as int64)
  --     when 0 then 'outbound'
  --     when 1 then 'inbound'
  --     else null
  --   end as direction_name

from src