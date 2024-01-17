{{ config(materialized='table', dist='pat_key') }}

select
    flowsheet_all.pat_key,
    flowsheet_all.flowsheet_id,
    lower(flowsheet_all.flowsheet_name) as flowsheet_name,
    flowsheet_all.flowsheet_record_id,
    lower(flowsheet_all.meas_val) as meas_val,
    flowsheet_all.meas_val_num,
    flowsheet_all.recorded_date
from
    {{ ref('flowsheet_all') }} as flowsheet_all
where
    flowsheet_all.pat_key in (select pat_key from {{ ref('stg_outbreak_pui_cohort_union') }})
