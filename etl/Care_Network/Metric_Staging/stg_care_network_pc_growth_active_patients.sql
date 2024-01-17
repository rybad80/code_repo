select
    {{
        dbt_utils.surrogate_key([
            'pat_key',
            'month_year'
        ])
    }} as primary_key,
    'Total Active Patients' as metric_name,
    'pc_growth_active_patients' as metric_id,
    pat_id,
    pat_key,
    mrn,
    month_year,
    last_well_date,
    pcp_name,
    department_name,
    pc_active_patient_ind

from 
    {{ref('care_network_primary_care_active_patients')}}

where
    pc_active_patient_ind = 1
    and month_year < date_trunc('month', current_date)
