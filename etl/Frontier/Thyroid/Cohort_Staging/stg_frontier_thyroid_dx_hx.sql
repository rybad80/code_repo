--region: create some dx indicators for later
select
    cohort_base_tmp.pat_key,
    diagnosis_encounter_all.mrn,
    min(case when diagnosis_encounter_all.icd10_code = 'C73'
        then diagnosis_encounter_all.encounter_date else null end) as thyroid_cancer_dx_date,
    min(case when diagnosis_encounter_all.icd10_code in ('C73', --thyroid cancer
                                                        'E04.1', --thyroid nodules
                                                        'E04.2', --thyroid nodules
                                                        'E06.3', --autoimmune thyroiditis/hashimoto’s
                                                                --thyroiditis
                                                        'E05.00', --graves’ disease
                                                        'E05.90' --graves’ disease
                                                        )
            or snomed_concept.snomed_concept_id in ('SNOMED#722859001', --PTEN hamartoma tumor syndrome (disorder)
                                                    --below are children of '722859001'
                                                    'SNOMED#234138005', -- Bannayan syndrome (disorder)
                                                    'SNOMED#58037000', --Cowden syndrome (disorder)
                                                    'SNOMED#716862002', --Proteus like syndrome (disorder)
                                                    'SNOMED#763867001' --Segmental outgrowth, lipomatosis,
                                                                --arteriovenous malformation,
                                                                --epidermal nevus syndrome (disorder)
                                                    )
        then diagnosis_encounter_all.encounter_date else null end) as thyroid_center_dx_date
from {{ ref('stg_frontier_thyroid_cohort_base_tmp') }} as cohort_base_tmp
inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
    on cohort_base_tmp.pat_key = diagnosis_encounter_all.pat_key
left join {{source('cdw', 'diagnosis_clinical_concept_snomed')}} as diagnosis_clinical_concept_snomed
    on diagnosis_encounter_all.dx_key = diagnosis_clinical_concept_snomed.dx_key
left join {{source('cdw', 'snomed_concept')}} as snomed_concept
    on diagnosis_clinical_concept_snomed.snomed_concept_key = snomed_concept.snomed_concept_key
where
    diagnosis_encounter_all.icd10_code in ('C73', --thyroid cancer
                                            'E04.1', --thyroid nodules
                                            'E04.2', --thyroid nodules
                                            'E06.3', --autoimmune thyroiditis/hashimoto’s thyroiditis
                                            'E05.00', --graves’ disease
                                            'E05.90' --graves’ disease
                                            )
    or snomed_concept.snomed_concept_id in ('SNOMED#722859001', --PTEN hamartoma tumor syndrome (disorder)
                                            --below are children of '722859001' --PTEN
                                            'SNOMED#234138005', -- Bannayan syndrome (disorder)
                                            'SNOMED#58037000', --Cowden syndrome (disorder)
                                            'SNOMED#716862002', --Proteus like syndrome (disorder)
                                            'SNOMED#763867001' --Segmental outgrowth, lipomatosis, arteriovenous
                                                        --malformation, epidermal nevus syndrome (disorder)
                                            )
group by cohort_base_tmp.pat_key,
    diagnosis_encounter_all.mrn
