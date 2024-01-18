select distinct
    neo_nicu_episode.pat_key,
    lookup_neo_nicu_diagnosis.diagnosis_name as cohort_group_name,
    lookup_neo_nicu_diagnosis.diagnosis_display_name as cohort_group_display_name,
    neo_nicu_episode.dob as cohort_group_enter_date
from
    {{ ref('neo_nicu_episode') }} as neo_nicu_episode
    inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        on diagnosis_encounter_all.visit_key = neo_nicu_episode.visit_key
    inner join {{ ref('lookup_neo_nicu_diagnosis') }} as lookup_neo_nicu_diagnosis
        on lookup_neo_nicu_diagnosis.diagnosis_id = diagnosis_encounter_all.diagnosis_id
where
    lookup_neo_nicu_diagnosis.diagnosis_name != 'cdh'
    /* only look at problem list for CDH cohort */
    or (
        lookup_neo_nicu_diagnosis.diagnosis_name = 'cdh'
        and diagnosis_encounter_all.source_summary like '%problem_list%'
    )
