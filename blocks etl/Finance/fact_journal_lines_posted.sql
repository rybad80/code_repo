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
    {{ ref('fact_journal_lines')}}
where 1 = 1
and lower(journal_status) = 'posted'
