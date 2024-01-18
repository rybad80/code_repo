{{ config(meta = {
    'critical': true
}) }}

select
    stg_encounter.visit_key as index_visit_key,
    stg_encounter.csn as index_csn,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as index_hsp_acct_key,
    stg_encounter.pat_key,
    stg_patient_ods.mrn,
    stg_patient_ods.patient_name,
    stg_encounter.patient_class as index_patient_class,
    stg_encounter.age_years as index_admit_age,
    stg_encounter_payor.payor_name as index_primary_payor,
    stg_encounter.hospital_admit_date as index_hospital_admit_date,
    stg_encounter.hospital_discharge_date as index_hospital_discharge_date,
    department.dept_nm as index_primary_department,
    admit_dept.department_name as index_admit_department,
    discharge_dept.department_name as index_discharge_department,
    primary_proc.proc_nm as index_primary_procedure,
    extract(
        epoch from stg_encounter.hospital_discharge_date - stg_encounter.hospital_admit_date
    ) / 86400.0 as index_los_days,
    extract(epoch from current_date - stg_encounter.hospital_discharge_date) / 86400.0 as days_since_discharge,
    case
        when stg_patient_ods.death_date is null then 0
        when stg_patient_ods.death_date < stg_encounter.hospital_discharge_date then 1
        else 0
    end as deceased_ind
from
    {{ref('stg_encounter')}} as stg_encounter
    inner join {{ref('stg_patient_ods')}} as stg_patient_ods
        on stg_patient_ods.patient_key = stg_encounter.patient_key
    inner join {{source('cdw', 'department')}} as department
        on department.dept_key = stg_encounter.dept_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter_payor.visit_key = stg_encounter.visit_key
    left join {{source('cdw', 'hospital_account_diag_icd')}} as primary_proc
        on coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) = primary_proc.hsp_acct_key
            and primary_proc.line = 1
    inner join {{ref('stg_adt_all')}} as admit_dept
        on stg_encounter.visit_key = admit_dept.visit_key
            and admit_dept.all_department_order = 1
    left join {{ref('stg_adt_all')}} as discharge_dept
        on stg_encounter.visit_key = discharge_dept.visit_key
            and discharge_dept.last_department_ind = 1
where
    -- Removing 6 West Special Delivery Unit (22), Emergency Department 10292012, Periop Complex 101001069
    -- Removing Rehab departments units 27 CSH3W REHAB and 10028 3ECSH REHAB
    department.dept_id not in (
        22, 101001069, 10292012, 27, 10028
    )
    -- INPATIENT OR OBSERVATION ONLY
    and stg_encounter.patient_class_id in ('1', '5')
    -- WANT DISCHARGED VISITS ONLY
    and stg_encounter.hospital_discharge_date is not null
