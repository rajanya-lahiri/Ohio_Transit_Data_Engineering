{{ config(materialized='view') }}

with src as (
  select * from {{ source('gtfs_raw','stops') }}
)

select
  cast(stop_id as string) as stop_id,
  nullif(trim(cast(stop_code as string)), '') as stop_code,
  nullif(trim(cast(stop_name as string)), '') as stop_name,
  nullif(trim(cast(stop_desc as string)), '') as stop_desc,
  safe_cast(stop_lat as float64) as stop_lat,
  safe_cast(stop_lon as float64) as stop_lon,
  nullif(trim(cast(zone_id as string)), '') as zone_id,
  nullif(trim(cast(stop_url as string)), '') as stop_url,
  cast(nullif(cast(location_type as string), '') as int64) as location_type,
  nullif(trim(cast(parent_station as string)), '') as parent_station,
  nullif(trim(cast(stop_timezone as string)), '') as stop_timezone,
  cast(nullif(cast(wheelchair_boarding as string), '') as int64) as wheelchair_boarding,
  case
    when safe_cast(stop_lon as float64) is not null
         and safe_cast(stop_lat as float64) is not null
    then st_geogpoint(safe_cast(stop_lon as float64), safe_cast(stop_lat as float64))
  end as geog
from src