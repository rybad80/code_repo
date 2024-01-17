{{
    config(
        materialized = 'view'
    )
}}

select
    or_proc_id,
    rpt_grp2_c,
    rpt_grp4_c,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
from
    {{ ref('nhsn_procedure_codes_snapshot') }}
