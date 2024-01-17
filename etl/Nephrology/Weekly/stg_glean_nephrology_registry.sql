with registry as (--region
    select distinct --sometimes duplicates if 1 dx_id is linked to 2+ rows
        registry_metrics.pat_key,
        mrn,
        stg_patient.dob,
        current_age,
        current_age * 12 as age_in_months,
        stg_patient.sex,
        'CHOP' as facility,
        current_date as today,
        last_neph_visit,
        provider.full_nm as last_neph_prov,
        next_neph_appt,
        dx_nm as gd_primary_dx_last_neph_visit,
        phenotype,
        kidney_biopsy_date,
        kidney_biopsy_result,
        genetic_testing_performed,
        remission_status,
        remission_status_date,
        urine_protein,
        last_urinalysis_3yr,
        admission_count_past_30_days,
        ip_days_past_30_days,
        revisit_7_day_acute_3_month,
        last_covid_19_vaccine,
        most_recent_flu_vaccine_,
        most_recent_pneumovax,
        second_most_recent_pneumovax,
        most_recent_prevnar_13,
        second_most_recent_prevnar_13,
        third_most_recent_prevnar_13,
        fourth_most_recent_prevnar_13,
        imm_rec_rev,
        tb,
        rd_counseling,
        patient_family_education,
        department.dept_nm as last_nephrology_department_name
    from
        {{ ref('stg_glean_nephrology_registry_metrics')}} as registry_metrics
        left join {{ ref('stg_glean_nephrology_smartphrases')}} as smartphrases
            on registry_metrics.pat_key = smartphrases.pat_key
        left join {{source('cdw', 'provider')}} as provider
            on registry_metrics.last_neph_prov = provider.prov_id
        left join {{source('cdw', 'diagnosis')}} as diagnosis
            on registry_metrics.gd_primary_dx_last_neph_visit = diagnosis.dx_id
        inner join {{ ref('stg_patient') }} as stg_patient
            on registry_metrics.pat_key = stg_patient.pat_key
        inner join {{source('cdw', 'department')}} as department
            on registry_metrics.last_nephrology_department_id = department.dept_id
--end region
)

select
    *
from
    registry
