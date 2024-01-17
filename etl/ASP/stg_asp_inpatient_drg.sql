with hospital_account_drg as (
    select distinct
        hospital_account_drg.hsp_acct_key,
        hospital_account_drg.drg_key

    from
        {{source('cdw', 'hospital_account_drg')}} as hospital_account_drg

    where
        hospital_account_drg.dict_drg_type_key in (
            419,
            429,    -- DRG APR V25
            35705,  -- DRG APR V27
            137705, -- DRG APR V29
            185706, -- DRG APR V30
            351805  -- DRG APR V31
        )
)

select
    stg_asp_inpatient_cohort.visit_key,
    hospital_account_visit.hsp_acct_key,
    diagnosis_group.drg_num,
    diagnosis_group.drg_nm,
    to_number(substr(diagnosis_group.drg_num, 4, 3), '999') as apr_drg,
    substr(diagnosis_group.drg_num, 7) as drg_soi

from
    {{ref('stg_asp_inpatient_cohort')}}                    as stg_asp_inpatient_cohort
    inner join {{source('cdw', 'hospital_account_visit')}} as hospital_account_visit
        on stg_asp_inpatient_cohort.visit_key = hospital_account_visit.visit_key
    left join hospital_account_drg
        on hospital_account_visit.hsp_acct_key = hospital_account_drg.hsp_acct_key
    inner join {{source('cdw', 'diagnosis_group')}}        as diagnosis_group
        on hospital_account_drg.drg_key = diagnosis_group.drg_key
