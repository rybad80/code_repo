{{ config(materialized='table', dist='patient_key') }}

select
    diabetes_visit_cohort.patient_key,
    stg_diabetes_icr_active_flowsheets.recorded_date,
    stg_diabetes_icr_active_flowsheets.meas_val,
    stg_diabetes_icr_active_flowsheets.np_prov_key,
    stg_diabetes_icr_active_flowsheets.fs_type
from
    {{ ref('diabetes_visit_cohort') }} as diabetes_visit_cohort
    --some SDE doc didn't include valid visit_key in TDL:
    inner join {{ ref('stg_diabetes_icr_active_flowsheets') }} as stg_diabetes_icr_active_flowsheets
        on stg_diabetes_icr_active_flowsheets.pat_key = diabetes_visit_cohort.pat_key
where
    lower(diabetes_visit_cohort.appt_stat) in ('arrived', 'completed')
group by
    diabetes_visit_cohort.patient_key,
    stg_diabetes_icr_active_flowsheets.recorded_date,
    stg_diabetes_icr_active_flowsheets.meas_val,
    stg_diabetes_icr_active_flowsheets.np_prov_key,
    stg_diabetes_icr_active_flowsheets.fs_type
