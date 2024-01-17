select
    stg_encounter_outpatient.visit_key,
    stg_encounter_outpatient.department_name as drill_down,
    'Median New Patient Lag Time - Physician/APP Visit (Days)' as metric_name,
    'pc_growth_new_patient_lag_time' as metric_id,
    stg_encounter_outpatient.patient_name,
    stg_encounter_outpatient.appointment_made_date as encounter_date,
    stg_encounter_outpatient.age_years,
    stg_encounter_outpatient.age_months,
    stg_encounter_outpatient.age_days,
    stg_encounter_outpatient.provider_name,
    stg_encounter_outpatient.department_name,
    stg_encounter_outpatient.department_id,
    stg_encounter_outpatient.npv_appointment_lag_days as scheduled_to_encounter_days,
    stg_encounter_outpatient.pat_key,
    stg_encounter_outpatient.dept_key
from
    {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
where
    stg_encounter_outpatient.npv_lag_incl_ind = 1
    and stg_encounter_outpatient.primary_care_ind = 1
