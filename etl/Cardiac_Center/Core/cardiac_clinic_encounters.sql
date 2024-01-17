select
    stg_encounter.visit_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.sex,
    stg_encounter.age_years,
    stg_encounter.age_days,
    initcap(provider.full_nm) as provider_name,
    provider.prov_id as provider_id,
    department.dept_nm as department_name,
    department.dept_id as department_id,
    stg_encounter_payor.payor_name,
    stg_encounter_payor.payor_group,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    stg_encounter.appointment_date,
    stg_encounter.appointment_status,
    group_concat(diagnosis_encounter_all.diagnosis_name, '; ') as primary_visit_dx,
    stg_encounter.dept_key,
    stg_encounter.prov_key,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from
    {{ref('stg_encounter')}} as stg_encounter
    inner join {{source('cdw', 'department')}} as department
        on stg_encounter.dept_key = department.dept_key
    inner join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter_payor.visit_key = stg_encounter.visit_key
    left join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on stg_encounter.visit_key = diagnosis_encounter_all.visit_key
		and marked_primary_ind = 1
		and visit_diagnosis_ind = 1
where
    (
        lower(department.dept_nm) like '%cardiology%'
        or lower(department.dept_nm) like '%ckdp%'
        or lower(department.dept_nm) like '%heart%'
    )
    and stg_encounter.encounter_type_id in (3, 50, 101)
group by
    stg_encounter.visit_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.sex,
    stg_encounter.age_years,
    stg_encounter.age_days,
    provider.full_nm,
    provider.prov_id,
    department.dept_nm,
    department.dept_id,
    stg_encounter_payor.payor_name,
    stg_encounter_payor.payor_group,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    stg_encounter.appointment_date,
    stg_encounter.appointment_status,
    stg_encounter.dept_key,
    stg_encounter.prov_key,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0)
