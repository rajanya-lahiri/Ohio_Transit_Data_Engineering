{{ config(materialized='view') }}

with src as (
  select * from {{ source('gtfs_raw','calendar') }}
)
select
  cast(service_id as string) as service_id,
  cast(nullif(cast(monday    as string), '') as int64) as monday,
  cast(nullif(cast(tuesday   as string), '') as int64) as tuesday,
  cast(nullif(cast(wednesday as string), '') as int64) as wednesday,
  cast(nullif(cast(thursday  as string), '') as int64) as thursday,
  cast(nullif(cast(friday    as string), '') as int64) as friday,
  cast(nullif(cast(saturday  as string), '') as int64) as saturday,
  cast(nullif(cast(sunday    as string), '') as int64) as sunday,
  SAFE.PARSE_DATE('%Y%m%d', cast(start_date as string)) as start_date,
  SAFE.PARSE_DATE('%Y%m%d', cast(end_date   as string)) as end_date
from src