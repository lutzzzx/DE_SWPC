with source as (
    select * from {{ source('silver', 'solar_wind_mag') }}
),

cleaned as (
    select
        time_tag as observed_at,
        bx_gsm,
        by_gsm,
        bz_gsm,
        bt
    from source
    where time_tag is not null
      and bt is not null
)

select distinct on (observed_at) *
from cleaned
order by observed_at
