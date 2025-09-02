-- {{ config(materialized='view') }}

-- -- Clean + type the raw GTFS routes table
-- with src as (
--   select * from {{ source('gtfs_raw', 'routes') }}
-- )

-- select
--   -- keys / ids
--   cast(route_id  as string) as route_id,          -- raw is INT64 → use STRING as the universal key type
--   cast(agency_id as string) as agency_id,         -- keep ids as strings for consistent joins

--   -- human fields (trim spaces, turn '' → NULL)
--   nullif(trim(cast(route_short_name as string)), '') as route_short_name,
--   nullif(trim(cast(route_long_name  as string)), ''),  as route_long_name,
--   nullif(trim(cast(route_desc       as string)), ''),  as route_desc,

--   -- route_type is a numeric GTFS code; normalize blanks → NULL, enforce INT64
--   cast(nullif(cast(route_type as string), '') as int64) as route_type,

--   -- optional URLs/colors (clean up)
--   nullif(trim(cast(route_url        as string)), '')             as route_url,
--   upper(nullif(trim(cast(route_color      as string)), ''))      as route_color_hex,       -- 'FFFFFF' style
--   upper(nullif(trim(cast(route_text_color as string)), ''))      as route_text_color_hex,

--   -- convenience: a readable mode name for route_type (common GTFS values)
--   case cast(nullif(cast(route_type as string), '') as int64)
--     when 0 then 'Tram/Light rail'
--     when 1 then 'Subway/Metro'
--     when 2 then 'Rail'
--     when 3 then 'Bus'
--     when 4 then 'Ferry'
--     when 5 then 'Cable tram'
--     when 6 then 'Aerial lift'
--     when 7 then 'Funicular'
--     when 11 then 'Trolleybus'
--     when 12 then 'Monorail'
--     else 'Other/Unknown'
--   end as route_mode_name

-- from src


{{ config(materialized='view') }}

-- Clean + type the raw GTFS routes table
with src as (
  select * from {{ source('gtfs_raw', 'routes') }}
)

select
  -- keys / ids
  cast(route_id  as string) as route_id,
  cast(agency_id as string) as agency_id,

  -- human fields (trim spaces, '' -> NULL)
  nullif(trim(cast(route_short_name as string)), '') as route_short_name,
  nullif(trim(cast(route_long_name  as string)), '') as route_long_name,
  nullif(trim(cast(route_desc       as string)), '') as route_desc,

  -- route_type is a numeric GTFS code; normalize blanks -> NULL, enforce INT64
  cast(nullif(cast(route_type as string), '') as int64) as route_type,

  -- optional URL/colors (clean up)
  nullif(trim(cast(route_url        as string)), '')        as route_url,
  upper(nullif(trim(cast(route_color      as string)), '')) as route_color_hex,
  upper(nullif(trim(cast(route_text_color as string)), '')) as route_text_color_hex,

  -- readable mode label
  case cast(nullif(cast(route_type as string), '') as int64)
    when 0 then 'Tram/Light rail'
    when 1 then 'Subway/Metro'
    when 2 then 'Rail'
    when 3 then 'Bus'
    when 4 then 'Ferry'
    when 5 then 'Cable tram'
    when 6 then 'Aerial lift'
    when 7 then 'Funicular'
    when 11 then 'Trolleybus'
    when 12 then 'Monorail'
    else 'Other/Unknown'
  end as route_mode_name

from src