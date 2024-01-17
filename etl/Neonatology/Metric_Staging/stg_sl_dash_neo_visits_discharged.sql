select
    *
from
    {{ ref('stg_sl_dash_neo_visits') }}
where
    hospital_discharged_ind = 1
