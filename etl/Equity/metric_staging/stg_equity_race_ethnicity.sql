{{
    config(
        materialized = 'view'
    )
}}

select
    *
from
    {{ ref('race_ethnicity_snapshot')}}
