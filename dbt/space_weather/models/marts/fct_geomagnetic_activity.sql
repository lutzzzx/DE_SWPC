with kp as (
    select * from {{ ref('stg_planetary_k_index') }}
),

mag as (
    select distinct on (date_trunc('minute', observed_at))
        date_trunc('minute', observed_at) as minute_bucket,
        bx_gsm,
        by_gsm,
        bz_gsm,
        bt
    from {{ ref('stg_solar_wind_mag') }}
    order by date_trunc('minute', observed_at), observed_at desc
),

joined as (
    select
        kp.observed_at,
        kp.kp_value,
        mag.bx_gsm,
        mag.by_gsm,
        mag.bz_gsm,
        mag.bt
    from kp
    left join mag
        on date_trunc('minute', kp.observed_at) = mag.minute_bucket
)

select
    observed_at,
    kp_value,
    bx_gsm,
    by_gsm,
    bz_gsm,
    bt,
    case
        when kp_value >= 9 then 'G5 - Extreme'
        when kp_value >= 8 then 'G4 - Severe'
        when kp_value >= 7 then 'G3 - Strong'
        when kp_value >= 6 then 'G2 - Moderate'
        when kp_value >= 5 then 'G1 - Minor'
        else 'G0 - Normal'
    end as storm_scale
from joined
