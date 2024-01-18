select
    encounter_primary_care.visit_key,
    encounter_primary_care.department_name as drill_down,
    'Lag Time - Well Visit Schedule to Encounter' as metric_name,
    'pc_growth_well_visit_lag_time' as metric_id,
    encounter_primary_care.patient_name,
    encounter_primary_care.encounter_date,
    encounter_primary_care.age_years,
    encounter_primary_care.age_months,
    encounter_primary_care.age_days,
    encounter_primary_care.provider_name,
    encounter_primary_care.department_name,
    encounter_primary_care.department_id,
    encounter_primary_care.well_visit_ind,
    encounter_primary_care.scheduled_to_encounter_days,
    encounter_primary_care.pat_key,
    encounter_primary_care.dept_key

from
    {{ref('encounter_primary_care')}} as encounter_primary_care

where
    encounter_primary_care.well_visit_ind = 1
    and encounter_primary_care.new_pc_patient_ind != 1
    and encounter_primary_care.age_years >= 1
