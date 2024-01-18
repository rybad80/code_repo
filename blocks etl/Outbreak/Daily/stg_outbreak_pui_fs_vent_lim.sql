{{ config(materialized='table', dist='pat_key') }}

select
    flowsheet_ventilation.encounter_date,
    flowsheet_ventilation.pat_key,
    lower(flowsheet_ventilation.resp_o2_device) as resp_o2_device,
    flowsheet_ventilation.invasive_ind,
    flowsheet_ventilation.recorded_date,
    flowsheet_ventilation.flowsheet_record_id
from
    {{ ref('flowsheet_ventilation') }} as flowsheet_ventilation
where
    flowsheet_ventilation.pat_key in (select pat_key from {{ ref('stg_outbreak_pui_cohort_union') }})
