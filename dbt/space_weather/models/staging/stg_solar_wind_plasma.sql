with source as (
    select * from {{ source('silver', 'solar_wind_plasma') }}
),

cleaned as (
    select
        time_tag as observed_at,
        speed,
        density,
        temperature
    from source
    where time_tag is not null
      and speed is not null
)

select distinct on (observed_at) *
from cleaned
order by observed_at
