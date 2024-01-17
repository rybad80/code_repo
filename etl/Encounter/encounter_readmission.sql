with index_diagnosis as (
    select
        index_visit.index_visit_key,
        diagnosis.dx_nm as index_primary_diagnosis
    from
        {{ref('stg_encounter_readmission_index')}} as index_visit
        inner join {{source('cdw', 'visit_diagnosis')}} as visit_diagnosis
            on visit_diagnosis.visit_key = index_visit.index_visit_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_dx_sts
            on dict_dx_sts.dict_key = visit_diagnosis.dict_dx_sts_key
            -- PRIMARY VISIT DIAGNOSIS
            and dict_dx_sts.src_id = 315
        inner join {{source('cdw', 'diagnosis')}} as diagnosis
            on diagnosis.dx_key = visit_diagnosis.dx_key
            and diagnosis.seq_num = 1
    -- grouped because of diagnosis dupes
    group by
        index_visit.index_visit_key,
        diagnosis.dx_nm
),

readmit_diagnosis as (
    select
        readmission.readmit_visit_key,
        diagnosis.dx_nm as readmit_primary_diagnosis
    from
        {{ref('stg_encounter_readmission_readmit')}} as readmission
        inner join {{source('cdw', 'visit_diagnosis')}} as visit_diagnosis
            on visit_diagnosis.visit_key = readmission.readmit_visit_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_dx_sts
            on dict_dx_sts.dict_key = visit_diagnosis.dict_dx_sts_key
            -- PRIMARY VISIT DIAGNOSIS
            and dict_dx_sts.src_id = 315
        inner join {{source('cdw', 'diagnosis')}} as diagnosis
            on diagnosis.dx_key = visit_diagnosis.dx_key
            and diagnosis.seq_num = 1
    -- grouped because of diagnosis dupes
    group by
        readmission.readmit_visit_key,
        diagnosis.dx_nm
)

-- FINAL SFW
select
    index_visit.index_visit_key,
    index_visit.index_csn,
    index_visit.index_hsp_acct_key,
    index_visit.pat_key,
    index_visit.mrn,
    index_visit.patient_name,
    index_visit.index_patient_class,
    index_visit.index_admit_age,
    index_visit.index_primary_payor,
    index_visit.index_hospital_admit_date,
    index_visit.index_hospital_discharge_date,
    index_visit.index_los_days,
    index_visit.index_primary_department,
    index_visit.index_admit_department,
    index_visit.index_discharge_department,
    index_visit.index_primary_procedure,
    index_diagnosis.index_primary_diagnosis,
    readmission.readmit_visit_key,
    readmission.readmit_csn,
    readmission.readmit_hsp_acct_key,
    readmission.readmit_patient_class,
    readmission.readmit_primary_payor,
    readmission.readmit_hospital_admit_date,
    readmission.readmit_hospital_discharge_date,
    readmission.readmit_primary_department,
    readmission.readmit_admission_department,
    readmission.readmit_discharge_department,
    readmission.readmit_primary_procedure,
    readmission.readmit_los_days,
    readmit_diagnosis.readmit_primary_diagnosis,
    readmission.days_to_readmission,
    case
     -- no false positives for transfers recorded as separate admissions
        when days_to_readmission < 0.01 then null
        when readmission.days_to_readmission <= 7 then 1
        when index_visit.days_since_discharge <= 7 then null
        else 0
    end as readmit_7_day_ind,
    -- no false positives for transfers recorded as separate admissions
    case
        when days_to_readmission < 0.01 then null
        when readmission.days_to_readmission <= 14 then 1
        when index_visit.days_since_discharge <= 14 then null
        else 0
    end as readmit_14_day_ind,
    case
    -- no false positives for transfers recorded as separate admissions
        when days_to_readmission < 0.01 then null
        when readmission.days_to_readmission <= 30 then 1
        when index_visit.days_since_discharge <= 30 then null
        else 0
    end as readmit_30_day_ind,
    case
    -- no false positives for transfers recorded as separate admissions
        when days_to_readmission < 0.01 then null
        when readmission.days_to_readmission <= 90 then 1
        when index_visit.days_since_discharge <= 90 then null
        else 0
    end as readmit_90_day_ind,
    index_visit.deceased_ind,
    case
        when index_visit.index_primary_payor = readmission.readmit_primary_payor then 1 else 0
    end as same_primary_payor_ind,
    case
        when index_visit.index_primary_department = readmission.readmit_primary_department then 1 else 0
    end as same_primary_dept_ind,
    case
        when index_visit.index_admit_department = readmission.readmit_admission_department then 1 else 0
    end as same_admit_dept_ind,
    case
        when index_visit.index_discharge_department = readmission.readmit_discharge_department then 1 else 0
    end as same_discharge_dept_ind,
    case
        when index_visit.index_primary_procedure = readmission.readmit_primary_procedure then 1 else 0
    end as same_primary_procedure_ind,
    case
        when index_diagnosis.index_primary_diagnosis = readmit_diagnosis.readmit_primary_diagnosis then 1 else 0
    end as same_primary_diagnosis_ind

from
    {{ref('stg_encounter_readmission_index')}} as index_visit
    left join index_diagnosis on index_visit.index_visit_key = index_diagnosis.index_visit_key
    left join {{ref('stg_encounter_readmission_readmit')}} as readmission
        on index_visit.index_visit_key = readmission.index_visit_key
        and readmission.next_visit_order = 1
        and readmission.most_recent_readmit_order = 1
    left join readmit_diagnosis on readmission.readmit_visit_key = readmit_diagnosis.readmit_visit_key
