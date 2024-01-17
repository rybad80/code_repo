select
    stg_encounter_ed.visit_key,
    stg_encounter_ed.pat_key,
    'NEONATAL_HYPERBILIRUBINEMIA' as cohort,
    null as subcohort

from {{ ref('stg_encounter_ed') }} as stg_encounter_ed
    inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        on stg_encounter_ed.visit_key = diagnosis_encounter_all.visit_key

where
    lower(diagnosis_encounter_all.icd10_code) in (
        'e80.4',
        'e80.6',
        'p55.1',
        'p59.0',
        'p59.3',
        'p59.9',
        'r17',
        'r79.89',
        'z91.89'
    )
    and diagnosis_encounter_all.ed_primary_ind = 1
    and stg_encounter_ed.age_days < 15
    and year(stg_encounter_ed.encounter_date) >= year(current_date) - 5

group by
    stg_encounter_ed.visit_key,
    stg_encounter_ed.pat_key
