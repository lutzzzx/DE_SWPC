with source as (
    select * from {{ source('silver', 'goes_xrays') }}
),

cleaned as (
    select
        time_tag as observed_at,
        energy_band,
        flux
    from source
    where time_tag is not null
      and energy_band = '0.1-0.8nm'
      and flux is not null
)

select distinct on (observed_at) *
from cleaned
order by observed_at
