with proc as (  --noqa: L029
    select
        stg_procedure_billing_hb.pat_key,
        stg_procedure_billing_hb.tx_id,
        stg_procedure_billing_hb.visit_key,
        stg_procedure_billing_hb.service_date,
        stg_procedure_billing_hb.proc_key,
        stg_procedure_billing_hb.dx_key,
        stg_procedure_billing_hb.dx_seq_num,
        stg_procedure_billing_hb.svc_prov_key,
        stg_procedure_billing_hb.prov_specialty,
        stg_procedure_billing_hb.pos_cd,
        stg_procedure_billing_hb.bill_prov_key,
        stg_procedure_billing_hb.dept_key,
        stg_procedure_billing_hb.mod1_key,
        stg_procedure_billing_hb.mod2_key,
        stg_procedure_billing_hb.mod3_key,
        stg_procedure_billing_hb.source_summary
    from
        {{ref('stg_procedure_billing_hb')}} as stg_procedure_billing_hb
    union all
    select
        stg_procedure_billing_pb.pat_key,
        stg_procedure_billing_pb.tx_id,
        stg_procedure_billing_pb.visit_key,
        stg_procedure_billing_pb.service_date,
        stg_procedure_billing_pb.proc_key,
        stg_procedure_billing_pb.dx_key,
        stg_procedure_billing_pb.dx_seq_num,
        stg_procedure_billing_pb.svc_prov_key,
        stg_procedure_billing_pb.prov_specialty,
        stg_procedure_billing_pb.pos_cd,
        stg_procedure_billing_pb.bill_prov_key,
        stg_procedure_billing_pb.dept_key,
        stg_procedure_billing_pb.mod1_key,
        stg_procedure_billing_pb.mod2_key,
        stg_procedure_billing_pb.mod3_key,
        stg_procedure_billing_pb.source_summary
    from
        {{ref('stg_procedure_billing_pb')}} as stg_procedure_billing_pb
),

diag as (
    select
        diagnosis.dx_key,
        max(diagnosis.icd9_cd) as icd9_code,
        max(diagnosis.icd10_cd) as icd10_code,
        max(diagnosis.dx_nm) as diagnosis_name,
        max(diagnosis.dx_id) as diagnosis_id,
        max(diagnosis.ext_id) as external_diagnosis_id
    from
        {{source('cdw', 'diagnosis')}} as diagnosis
    where
        diagnosis.seq_num = 1
    group by
        diagnosis.dx_key
)

select
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_patient.dob,
    stg_patient.sex,
    (date(proc.service_date) - date(stg_patient.dob)) / 365.25 as age_years,
    proc.visit_key,
    proc.tx_id,
    proc.service_date,
    procedure.cpt_cd as cpt_code,
    procedure.proc_nm as procedure_name,
    diag.icd9_code,
    diag.icd10_code,
    diag.diagnosis_name,
    proc.dx_seq_num as diagnosis_seq_num,
    mod1.mod_id as modifier_1_id,
    mod1.mod_nm as modifier_1_name,
    mod2.mod_id as modifier_2_id,
    mod2.mod_nm as modifier_2_name,
    mod3.mod_id as modifier_3_id,
    mod3.mod_nm as modifier_3_name,
    proc.source_summary,
    diag.diagnosis_id,
    diag.external_diagnosis_id,
    initcap(service_provider.full_nm) as provider_name,
    proc.prov_specialty as provider_specialty,
    proc.pos_cd,
    service_provider.prov_id as provider_id,
    initcap(billing_provider.full_nm) as billing_provider_name,
    billing_provider.prov_id as billing_provider_id,
    department.dept_nm as department_name,
    cast(department.dept_id as bigint) as department_id,
    proc.mod1_key,
    proc.mod2_key,
    proc.mod3_key,
    stg_patient.patient_key,
    proc.pat_key,
    proc.dx_key,
    service_provider.prov_key,
    proc.dept_key
from
    proc --noqa: L029
inner join {{ref('stg_patient')}} as stg_patient
    on	proc.pat_key = stg_patient.pat_key
    and stg_patient.create_source = 'CLARITY'
inner join {{source('cdw', 'procedure')}} as procedure --noqa: L029
    on	proc.proc_key = procedure.proc_key
left join diag
    on	proc.dx_key = diag.dx_key
left join {{source('cdw', 'provider')}} as service_provider
    on	proc.svc_prov_key = service_provider.prov_key
left join {{source('cdw', 'provider')}} as billing_provider
    on	proc.bill_prov_key = billing_provider.prov_key
left join {{source('cdw', 'department')}} as department
    on	proc.dept_key = department.dept_key
left join {{source('cdw', 'master_modifier')}} as mod1
    on proc.mod1_key = mod1.mod_key
left join {{source('cdw', 'master_modifier')}} as mod2
    on proc.mod2_key = mod2.mod_key
left join {{source('cdw', 'master_modifier')}} as mod3
    on proc.mod3_key = mod3.mod_key
