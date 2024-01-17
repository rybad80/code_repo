select
    *
from
    {{ ref('stg_sl_dash_cardiac_admissions')}}
where
    admissions is not null
