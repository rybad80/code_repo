--visits where patient had complicating pulmonary conditions
--final cohort excludes CAP visits within 1 yr after CPC visit
select
    stg_encounter.visit_key,
    stg_encounter.pat_key,
    stg_encounter.hospital_admit_date
from
    {{ref('stg_encounter')}} as stg_encounter
    inner join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on stg_encounter.visit_key = diagnosis_encounter_all.visit_key
    inner join {{ref('lookup_asp_inpatient_cap_diagnoses')}} as pulmonary_diagnoses
        on diagnosis_encounter_all.icd10_code = pulmonary_diagnoses.icd10_code
where
    --cohort begins FY21, so stage table begins FY20 (1 year prior)
    stg_encounter.hospital_admit_date >= '2019-07-01'
    and diagnosis_encounter_all.visit_diagnosis_ind = 1 --based on ED CAP automart
    and pulmonary_diagnoses.pulmonary_ind = 1
group by
    stg_encounter.visit_key,
    stg_encounter.pat_key,
    stg_encounter.hospital_admit_date
