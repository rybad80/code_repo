{{
    config(
        materialized = 'view',
        meta = {
            'critical': true
        }
    )
}}

select
    *
from
    {{ ref('block_availability_snapshot')}}
