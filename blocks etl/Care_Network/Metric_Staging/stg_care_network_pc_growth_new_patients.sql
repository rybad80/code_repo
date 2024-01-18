select
    visit_key,
    department_name,
    patient_name,
    encounter_date,
    age_years,
    age_months,
    age_days,
    provider_name,
    department_id,
    well_visit_ind,
    scheduled_to_encounter_days,
    pat_key,
    dept_key,
    case
        when new_pc_patient_ind = 1 and first_newborn_encounter_ind != 1 then 1 else 0
    end as pc_sl_new_patient_ind

from
    {{ref('encounter_primary_care')}}
