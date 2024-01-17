select distinct
    diagnosis_encounter_all.mrn,
    diagnosis_encounter_all.pat_key,
    diagnosis_encounter_all.patient_name,
    diagnosis_encounter_all.dob,
    lookup_neuro_dx_grouping.dx_grouping,
    coalesce(lookup_neuro_dx_grouping.subgrouping, 'No subgroup') as subgrouping,
    min(encounter_date) over(partition by diagnosis_encounter_all.pat_key, lookup_neuro_dx_grouping.dx_grouping)
        as dx_grouping_first_dx_date,
    min(encounter_date) over(partition by diagnosis_encounter_all.pat_key,
        lookup_neuro_dx_grouping.dx_grouping, lookup_neuro_dx_grouping.subgrouping) as subgrouping_first_dx_date
from {{ ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
inner join {{ ref('lookup_neuro_dx_grouping')}} as lookup_neuro_dx_grouping
    on diagnosis_encounter_all.icd10_code like lookup_neuro_dx_grouping.dx_cd
