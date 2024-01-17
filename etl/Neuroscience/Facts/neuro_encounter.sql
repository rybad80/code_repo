with visit_dx_ind as (
    select
        diagnosis_encounter_all.visit_key,
        max(case when lower(lookup_neuro_dx_grouping.dx_grouping) = 'epilepsy/seizure' then 1
            else 0 end) as epilepsy_seizure_visit_ind,
        max(case when lower(lookup_neuro_dx_grouping.dx_grouping) = 'headache/migraine' then 1
            else 0 end) as headache_migraine_visit_ind
    from {{ ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        inner join {{ ref('lookup_neuro_dx_grouping')}} as lookup_neuro_dx_grouping
            on diagnosis_encounter_all.icd10_code like lookup_neuro_dx_grouping.dx_cd
    where
        visit_primary_ind = 1
    group by
        diagnosis_encounter_all.visit_key
),

patient_dx_ind as (
    select
        stg_neuro_encounter.visit_key,
        max(case when lower(stg_neuro_cohorts.dx_grouping) = 'brain tumor' then 1
            else 0 end) as brain_tumor_patient_ind
    from {{ ref('stg_neuro_encounter')}} as stg_neuro_encounter
        inner join {{ ref('stg_neuro_cohorts')}} as stg_neuro_cohorts
            on stg_neuro_encounter.pat_key = stg_neuro_cohorts.pat_key
            and lower(stg_neuro_cohorts.dx_grouping) = 'brain tumor'
            and encounter_date >= dx_grouping_first_dx_date
    group by
        stg_neuro_encounter.visit_key
)

select
    stg_neuro_encounter.visit_key,
    stg_neuro_encounter.mrn,
    stg_neuro_encounter.pat_key,
    stg_neuro_encounter.patient_name,
    stg_neuro_encounter.dob,
    stg_neuro_encounter.encounter_date,
    coalesce(brain_tumor_patient_ind, 0) as brain_tumor_patient_ind,
    coalesce(epilepsy_seizure_visit_ind, 0) as epilepsy_seizure_visit_ind,
    coalesce(headache_migraine_visit_ind, 0) as headache_migraine_visit_ind,
    inpatient_census_ind,
    office_visit_ind,
    notes_ind,
    neurology_ind,
    neurosurgery_ind,
    leukodystrophy_ind
from {{ ref('stg_neuro_encounter')}} as stg_neuro_encounter
    left join patient_dx_ind
        on stg_neuro_encounter.visit_key = patient_dx_ind.visit_key
    left join visit_dx_ind
        on stg_neuro_encounter.visit_key = visit_dx_ind.visit_key
where encounter_date >= '2015-01-01'
