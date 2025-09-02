{{ config(materialized='view') }}

-- Source table reference
with src as (
    select * from {{ source('gtfs_raw', 'stop_times') }}
)

select
    -- IDs as strings for consistent joins across tables
    cast(trip_id as string) as trip_id,
    cast(stop_id as string) as stop_id,

    -- Convert empty strings to NULL, keep as STRING for GTFS HH:MM:SS
    nullif(trim(arrival_time), '')   as arrival_time,
    nullif(trim(departure_time), '') as departure_time,

    -- Sequences and numeric fields
    cast(stop_sequence as int64) as stop_sequence,

    -- Optional description/label fields
    nullif(trim(cast(stop_headsign as string)), '') as stop_headsign,

    -- Pickup/drop-off types (GTFS codes: 0=regular, 1=none, 2=phone, 3=driver)
    cast(pickup_type as int64)    as pickup_type,
    cast(drop_off_type as int64)  as drop_off_type,

    -- Distance traveled along shape (km/miles depending on feed)
    cast(shape_dist_traveled as float64) as shape_dist_traveled,

    -- GTFS timepoint flag (0=approximate, 1=exact)
    cast(timepoint as int64) as timepoint

from src