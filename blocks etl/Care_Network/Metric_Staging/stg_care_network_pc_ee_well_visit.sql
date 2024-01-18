select
    {{
        dbt_utils.surrogate_key([
            'pat_key',
            'month_year'
        ])
    }} as primary_key,
    pat_id,
    pat_key,
    mrn,
    month_year,
    dob,
    age_years,
    age_months,
    last_well_date,
    pcp_id,
    pcp_name,
    department_id,
    department_name,
    pc_active_patient_ind,
    well_visit_needed_ind,
    required_well_completed_ind,
    case when required_well_completed_ind = 1 then pat_key else null end as pat_required_well_completed
    
from
   {{ref('care_network_primary_care_active_patients')}}

where
    well_visit_needed_ind = 1
    and age_months >= 9
