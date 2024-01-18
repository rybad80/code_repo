{{ config(materialized='table', dist='pat_key') }}

select
    cohort.pat_key,
    cohort.outbreak_type,
    'Treatable Diagnosis' as reason,
    diagnosis_encounter_all.encounter_date as start_date,
    diagnosis_encounter_all.encounter_date + cast('2 months' as interval) as end_date,
    group_concat(diagnosis_encounter_all.diagnosis_name) as reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_cohort') }} as cohort
    inner join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on cohort.pat_key = diagnosis_encounter_all.pat_key
where
    diagnosis_encounter_all.encounter_date >= '2020-01-01'
    and diagnosis_encounter_all.icd10_code like 'E43%'
group by
    cohort.pat_key,
    cohort.outbreak_type,
    diagnosis_encounter_all.encounter_date
