--2.  patient had any in-person encounter at chop (hospital encounter/office visit/surgery) with diagnoses below
select
    stg_encounter.visit_key,
    stg_encounter.pat_key,
    stg_encounter.mrn,
    stg_encounter.encounter_date
from {{ ref('stg_frontier_thyroid_cohort_base_tmp') }} as cohort_base_tmp
inner join {{ref('stg_encounter')}} as stg_encounter
    on cohort_base_tmp.pat_key = stg_encounter.pat_key
inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
    on stg_encounter.visit_key = diagnosis_encounter_all.visit_key
inner join {{source('cdw','provider')}} as provider
    on provider.prov_key = stg_encounter.prov_key
left join {{source('cdw', 'diagnosis_clinical_concept_snomed')}} as diagnosis_clinical_concept_snomed
    on diagnosis_encounter_all.dx_key = diagnosis_clinical_concept_snomed.dx_key
left join {{source('cdw', 'snomed_concept')}} as snomed_concept
    on diagnosis_clinical_concept_snomed.snomed_concept_key = snomed_concept.snomed_concept_key
inner join {{ref('lookup_frontier_program_diagnoses')}} as lookup_frontier_program_diagnoses
    on (diagnosis_encounter_all.icd10_code = cast(
            lookup_frontier_program_diagnoses.lookup_dx_id as nvarchar(20))
                and lookup_frontier_program_diagnoses.code_type = 'icd-10'
                and lookup_frontier_program_diagnoses.program = 'thyroid')
        or (snomed_concept.snomed_concept_id = cast(
            lookup_frontier_program_diagnoses.lookup_dx_id as nvarchar(20))
                and lookup_frontier_program_diagnoses.code_type = 'snomed'
                and lookup_frontier_program_diagnoses.program = 'thyroid')
where
    provider.prov_id in ('10352', --'bauer, andrew j.'
                                '2006317', --'robbins, stephanie l'
                                '5323', --'mostoufi moab, sogol',
                                '16489', --'kivel, courtney g',
                                '9810', --'laetsch, theodore',
                                '25535', --'meyers, kelly',
                                '2006349' --"o'reilly, stephanie  h"
                                )
    and year(add_months(stg_encounter.encounter_date, 6)) >= 2020
    and diagnosis_encounter_all.visit_diagnosis_ind = 1
    and stg_encounter.encounter_type_id in (
                                            3,   --'hospital encounter'
                                            101, --'office visit'
                                            51   --'surgery'
                                            )
group by stg_encounter.visit_key,
    stg_encounter.encounter_date,
    stg_encounter.mrn,
    stg_encounter.pat_key
