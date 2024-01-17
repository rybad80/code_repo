{{ config(materialized='table', dist='pat_key') }}

with dx_chronic as (
    select distinct
        diagnosis.dx_key,
        diagnosis.dx_nm
    from
        {{source('cdw', 'diagnosis')}} as diagnosis
    where
        (diagnosis.icd10_cd like 'Z94%' --Transplanted organ and tissue status
        or diagnosis.icd10_cd like 'D80%'
        or diagnosis.icd10_cd like 'D81%'
        or diagnosis.icd10_cd like 'D82%'
        or diagnosis.icd10_cd like 'D83%'
        or diagnosis.icd10_cd like 'D84%' --Certain disorders involving the immune mechanism
        or diagnosis.icd10_cd like 'B20%' --Human immunodeficiency virus [HIV] disease
        or diagnosis.icd10_cd like 'E84%' --Cystic fibrosis
        or diagnosis.icd10_cd like 'G35%' --Multiple sclerosis
        or diagnosis.icd10_cd like 'D57%' --Sickle-cell disorders
        )
        and diagnosis.icd10_cd != 'D57.3' -- NOT sickle cell trait
)

select
    cohort.pat_key,
    cohort.outbreak_type,
    'Chronic Diagnosis' as reason,
    min(diagnosis_encounter_all.encounter_date) as start_date,
    current_date as end_date,
    group_concat(dx_chronic.dx_nm) as reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_cohort') }} as cohort
    inner join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on cohort.pat_key = diagnosis_encounter_all.pat_key
    inner join dx_chronic on dx_chronic.dx_key = diagnosis_encounter_all.dx_key
where
    diagnosis_encounter_all.encounter_date >= '2020-01-01'
group by
    cohort.pat_key,
    cohort.outbreak_type
