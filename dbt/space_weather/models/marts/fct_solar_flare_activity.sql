with xray as (
    select * from {{ ref('stg_goes_xrays') }}
)

select
    observed_at,
    flux,
    case
        when flux >= 1.0e-4 then 'X'
        when flux >= 1.0e-5 then 'M'
        when flux >= 1.0e-6 then 'C'
        when flux >= 1.0e-7 then 'B'
        else 'A'
    end as flare_class,
    round(
        (flux / (10 ^ floor(log(flux))))::numeric, 1
    ) as flare_magnitude
from xray
where flux > 0
