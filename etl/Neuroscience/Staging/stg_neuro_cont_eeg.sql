with procedure_billing_raw as (
	select 
        procedure_billing.pat_key,
        procedure_billing.mrn,
        procedure_billing.patient_name,
        procedure_billing.visit_key,
        procedure_billing.cpt_code,
        procedure_billing.procedure_name,
        procedure_billing.service_date,
        procedure_billing.age_years,
        department.dept_nm as department_specialty,
        procedure_billing.provider_specialty,
        procedure_billing.provider_name
    from
        {{ref('procedure_billing')}} as procedure_billing
	left join 
		{{source('cdw', 'department')}} as department
		on procedure_billing.department_id = department.dept_id
    where
        procedure_billing.source_summary = 'physician billing'
    group by
        procedure_billing.pat_key,
        procedure_billing.mrn,
        procedure_billing.patient_name,
        procedure_billing.visit_key,
        procedure_billing.cpt_code,
        procedure_billing.procedure_name,
        procedure_billing.service_date,
        procedure_billing.age_years,
        department.dept_nm,
        procedure_billing.provider_specialty,
        procedure_billing.provider_name
)

select
    procedure_billing_raw.pat_key,
    procedure_billing_raw.mrn,
    procedure_billing_raw.patient_name,
    procedure_billing_raw.visit_key,
    procedure_billing_raw.cpt_code,
    procedure_billing_raw.procedure_name,
    procedure_billing_raw.service_date,
    procedure_billing_raw.age_years,
    procedure_billing_raw.department_specialty,
    procedure_billing_raw.provider_specialty,
    procedure_billing_raw.provider_name,
    stg_patient.dob,
    master_visit_type.visit_type_nm,
    --amb video eeg hu & ambulatory hook up
    case when lookup_neuroscience_metric_cpt.cpt_grouping = 'Check' and visit_type_id in (3246, 3215)
            then 'Ambulatory'
        when lookup_neuroscience_metric_cpt.cpt_grouping = 'Check' and visit_type_id not in (3246, 3215) 
            then 'Inpatient'
        else cpt_grouping
        end as drill_down,
    case when lower(drill_down) in ('ambulatory', 'routine')
            then 'h9b'
        when lower(drill_down) = 'inpatient'
            then 'h9c'
        else null
        end as usnews_metric_id,
    {{
        dbt_utils.surrogate_key([
            'procedure_billing_raw.pat_key',
            'procedure_billing_raw.service_date'
        ])
    }} as count_eeg_days_id
from procedure_billing_raw
    left join {{ source('cdw', 'visit')}} as visit
        on visit.visit_key = procedure_billing_raw.visit_key
    left join {{ source('cdw', 'master_visit_type')}} as master_visit_type 
        on visit.appt_visit_type_key = master_visit_type.visit_type_key
    inner join {{ ref('lookup_neuroscience_metric_cpt')}} as lookup_neuroscience_metric_cpt
        on procedure_billing_raw.cpt_code = cast(lookup_neuroscience_metric_cpt.cpt_code as char(5))
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = procedure_billing_raw.pat_key
where
    lookup_neuroscience_metric_cpt.metric = 'continuous eeg'
