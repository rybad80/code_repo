{{
    config(materialized = 'view')
}}

select
    {{
        dbt_utils.surrogate_key([
            'pat_key',
            'dx_key',
            'med_key',
            'proc_key'
            ])
    }} as dx_med_proc_key,
    *
from
    {{ ref('stg_diagnosis_medically_complex_timeframe')}}
