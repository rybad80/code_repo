with cancer_dx as (
    --region any patients with an oncology diagnosis
    select diagnosis_encounter_all.pat_key
    from
        {{source('cdw', 'epic_grouper_item')}} as epic_grouper_item
        inner join {{source('cdw', 'epic_grouper_diagnosis')}} as epic_grouper_diagnosis
            on epic_grouper_item.epic_grouper_key = epic_grouper_diagnosis.epic_grouper_key
        inner join {{source('cdw', 'diagnosis')}} as diagnosis
            on epic_grouper_diagnosis.dx_key = diagnosis.dx_key
        inner join {{ref ('diagnosis_encounter_all')}} as diagnosis_encounter_all
            on diagnosis_encounter_all.dx_key = diagnosis.dx_key
        inner join {{source('cdw', 'patient')}} as patient
            on diagnosis_encounter_all.pat_key = patient.pat_key
    where lower(epic_grouper_item.epic_grouper_nm) like '%oncology%'
        --remove invalid mrns
        and patient.cur_rec_ind = 1
        and diagnosis_encounter_all.encounter_date >= '2011-01-01'
    group by
        diagnosis_encounter_all.pat_key
    --endregion
),

base_cohort_stage as (--region 
    /*any patients with an oncology diagnosis or any patients with a completed onco visit that is face to face*/
    select
        stg_cancer_center_visit.pat_key,
        1 as onco_visit_ind,
        0 as cancer_dx_ind
    from
        {{ref ('stg_cancer_center_visit')}} as stg_cancer_center_visit
    union all
    select
        pat_key,
        0 as onco_visit_ind,
        1 as cancer_dx_ind
    from cancer_dx
--endregion
),

base_cohort as (
    select
        pat_key,
        max(onco_visit_ind) as onco_visit_ind,
        max(cancer_dx_ind) as cancer_dx_ind
    from
        base_cohort_stage
    group by
        pat_key
)

select
    base_cohort.pat_key,
    stg_patient.mrn,
    stg_patient.death_date,
    stg_patient.dob,
    onco_visit_ind,
    cancer_dx_ind
from
    base_cohort
    inner join {{ref('stg_patient')}} as stg_patient
            on base_cohort.pat_key = stg_patient.pat_key
