select
    *
from
    {{ ref('stg_sl_dash_neo_episodes') }}
where
    nicu_discharged_ind = 1
