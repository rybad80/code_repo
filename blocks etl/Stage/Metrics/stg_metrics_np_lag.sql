{{ config(meta = {
    'critical': false
}) }}

select
    'clinical' as domain, -- noqa: L029
    'Median New Patient Lag Time - Physician/APP/Psychologist Visits (Days)' as metric_name,
    stg_encounter_outpatient.visit_key as primary_key,
    stg_encounter_outpatient.appointment_made_date as metric_date,
    stg_encounter_outpatient.npv_appointment_lag_days as num,
    stg_encounter_outpatient.npv_lag_incl_ind as denom,
    'median' as num_calculation,
    'median' as metric_type,
    'down' as desired_direction,
    'enterprise_sc_new_pat_lag' as metric_id,
    initcap(stg_encounter_outpatient.specialty_name) as specialty_name,
    stg_encounter_outpatient.department_name as department_name,
    stg_encounter_outpatient.dept_key,
    stg_encounter_outpatient.department_center,
    case
        when lower(department_care_network.revenue_location_group) in ('chca', 'csa')
            then 1
        else 0
    end as chca_csa_ind,
    coalesce(diagnosis_encounter_all.icd10_code, 'No visit diagnosis code') as primary_diagnosis_code,
    coalesce(diagnosis_encounter_all.diagnosis_name, 'No visit diagnosis') as primary_diagnosis
from
    {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
inner join
    {{ref('department_care_network')}} as department_care_network
    on stg_encounter_outpatient.dept_key = department_care_network.dept_key
left join
    {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
    on stg_encounter_outpatient.visit_key = diagnosis_encounter_all.visit_key
    and diagnosis_encounter_all.visit_primary_ind = 1
where
    stg_encounter_outpatient.npv_lag_incl_ind = 1
    and stg_encounter_outpatient.intended_use_id = 1009
