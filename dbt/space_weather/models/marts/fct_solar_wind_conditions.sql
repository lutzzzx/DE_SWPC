with plasma as (
    select * from {{ ref('stg_solar_wind_plasma') }}
),

mag as (
    select distinct on (date_trunc('minute', observed_at))
        date_trunc('minute', observed_at) as minute_bucket,
        bz_gsm,
        bt
    from {{ ref('stg_solar_wind_mag') }}
    order by date_trunc('minute', observed_at), observed_at desc
)

select
    plasma.observed_at,
    plasma.speed,
    plasma.density,
    plasma.temperature,
    mag.bz_gsm,
    mag.bt,
    case
        when plasma.speed >= 500 then true
        else false
    end as is_high_speed_stream
from plasma
left join mag
    on date_trunc('minute', plasma.observed_at) = mag.minute_bucket
