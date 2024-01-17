{{ config(meta = {
    'critical': true
}) }}

select
    index_visit.index_visit_key,
    readmission.visit_key as readmit_visit_key,
    stg_hsp_acct_xref.hsp_acct_key as readmit_hsp_acct_key,
    stg_readmission.csn as readmit_csn,
    stg_readmission.patient_class as readmit_patient_class,
    stg_readmission_payor.payor_name as readmit_primary_payor,
    readmission.hosp_admit_dt as readmit_hospital_admit_date,
    readmission.hosp_dischrg_dt as readmit_hospital_discharge_date,
    primary_proc.proc_nm as readmit_primary_procedure,
    readmit_dept.dept_nm as readmit_primary_department,
    admit_dept.department_name as readmit_admission_department,
    case
        when readmission.hosp_dischrg_dt is not null then discharge_dept.department_name
        else null
        end as readmit_discharge_department,
    extract(
        epoch from readmission.hosp_dischrg_dt - readmission.hosp_admit_dt
    ) / 86400.0 as readmit_los_days,
    extract(
        epoch from readmission.hosp_admit_dt - index_visit.index_hospital_discharge_date
    ) / 86400.0 as days_to_readmission,
    row_number()
        over(partition by index_visit.index_visit_key
        order by readmission.hosp_admit_dt) as next_visit_order,
    row_number()
        over(partition by readmission.visit_key
        order by index_visit.index_hospital_admit_date desc) as most_recent_readmit_order
from
    {{ref('stg_encounter_readmission_index')}} as index_visit
    inner join {{source('cdw', 'visit')}} as readmission
        on index_visit.pat_key = readmission.pat_key
        -- readmission must occur after discharge of index admission
        and index_visit.index_hospital_discharge_date <= readmission.hosp_admit_dt
    inner join {{ref('stg_encounter')}} as stg_readmission
            on stg_readmission.visit_key = readmission.visit_key
    inner join {{ref('stg_adt_all')}} as stg_adt_all
        on stg_adt_all.visit_key = readmission.visit_key
    left join {{ref('stg_encounter_payor')}} as stg_readmission_payor
        on stg_readmission_payor.visit_key = readmission.visit_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.visit_key = index_visit.index_visit_key
    left join {{source('cdw', 'hospital_account_diag_icd')}} as har_prc
        on stg_hsp_acct_xref.hsp_acct_key = har_prc.hsp_acct_key
    inner join {{source('cdw', 'department')}} as readmit_dept
        on readmit_dept.dept_key = stg_readmission.dept_key
    left join {{source('cdw', 'hospital_account_diag_icd')}} as primary_proc
        on primary_proc.hsp_acct_key = stg_hsp_acct_xref.hsp_acct_key
            and primary_proc.line = 1
    left join {{ref('stg_adt_all')}} as admit_dept
        on admit_dept.visit_key = readmission.visit_key
            and admit_dept.all_department_order = 1
    left join {{ref('stg_adt_all')}} as discharge_dept
        on discharge_dept.visit_key = readmission.visit_key
            and discharge_dept.last_department_ind = 1
where
    lower(readmission.hosp_admit_type) not like '%elective%'
    -- Removing 6 West Special Delivery Unit (22), Emergency Department 10292012, Periop Complex 101001069
    -- Removing Rehab departments units 27 CSH3W REHAB and 10028 3ECSH REHAB
    and readmit_dept.dept_id not in (
        22, 101001069, 10292012, 27, 10028
    )
    -- inpatient and observation ONLY
    and stg_readmission.patient_class_id in ('1', '5')
    --removing specific chemo procedures
    -- SKIN CHEMOSURGERY, IMPLANT CHEMOTHERA AGENT, IMMUNOTHERAPY AS ANTINEO, INJECT CA CHEMOTHER NEC
    and coalesce(har_prc.ref_cd, '') not in ('00.10', '99.25', '99.28', '86.24')
group by
    index_visit.index_visit_key,
    readmission.visit_key,
    stg_hsp_acct_xref.hsp_acct_key,
    stg_readmission.csn,
    stg_readmission.patient_class,
    stg_readmission_payor.payor_name,
    readmission.hosp_admit_dt,
    readmission.hosp_dischrg_dt,
    index_visit.index_hospital_admit_date,
    index_visit.index_hospital_discharge_date,
    primary_proc.proc_nm,
    readmit_dept.dept_nm,
    admit_dept.department_name,
    discharge_dept.department_name
