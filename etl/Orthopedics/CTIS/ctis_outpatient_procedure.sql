select
    procedure_order_clinical.proc_ord_key,
    procedure_order_clinical.mrn,
    procedure_order_clinical.patient_name,
    procedure_order_clinical.dob,
    procedure_order_clinical.encounter_date,
    procedure_order_clinical.department_name,
    provider.prov_id as provider_id,
    initcap(provider.full_nm) as provider_name,
    procedure_order_clinical.cpt_code,
    procedure_order_clinical.procedure_id,
    procedure_order_clinical.procedure_name,
    case when procedure_order_clinical.procedure_id = 938 then 'lengthening' else 'cast' end as procedure_type,
    case when procedure_order_clinical.procedure_id = 938 then 1 else 0 end as lengthening_ind,
    case when procedure_order_clinical.procedure_id != 938 then 1 else 0 end as cast_ind,
    procedure_order_clinical.placed_date,
    master_date.c_yyyy as calendar_year,
    master_date.f_yyyy as fiscal_year,
    master_date.fy_yyyy_qtr as fiscal_quarter,
    procedure_order_clinical.pat_key,
    procedure_order_clinical.visit_key,
    procedure_order_clinical.dept_key,
    stg_encounter.prov_key
from
    {{ ref('procedure_order_clinical') }} as procedure_order_clinical
    left join {{ ref('encounter_office_visit_completed') }} as encounter_office_visit_completed
        on encounter_office_visit_completed.visit_key = procedure_order_clinical.visit_key
    left join {{ ref('ctis_registry') }} as ctis_registry
        on ctis_registry.pat_key = procedure_order_clinical.pat_key
    left join {{ ref('stg_encounter') }} as stg_encounter
        on stg_encounter.visit_key = procedure_order_clinical.visit_key
    left join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ source('cdw', 'master_date') }} as master_date
        on master_date.full_dt = procedure_order_clinical.encounter_date
where
    lower(encounter_office_visit_completed.specialty) = 'orthopedics'
    and lower(procedure_order_clinical.order_status) = 'completed'
    and procedure_order_clinical.procedure_id in (
        938, --'SPINE SURGERY PROCEDURE UNLISTED'
        1995, --'APPLY BODY CAST,RISSER JACKET'
        1999 --'APPLY BODY CAST,SHLDR-HIP'
    )
