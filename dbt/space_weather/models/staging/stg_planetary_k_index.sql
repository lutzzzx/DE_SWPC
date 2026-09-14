with source as (
    select * from {{ source('silver', 'planetary_k_index') }}
),

cleaned as (
    select
        time_tag                         as observed_at,
        coalesce(kp_index, estimated_kp) as kp_value,
        kp_source
    from source
    where time_tag is not null
      and coalesce(kp_index, estimated_kp) is not null
)

select distinct on (observed_at) *
from cleaned
order by observed_at, kp_value desc
