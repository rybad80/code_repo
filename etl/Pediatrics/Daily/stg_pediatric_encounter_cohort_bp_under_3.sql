{{ config(meta = {
    'critical': true
}) }}

select distinct
        stg_encounter_outpatient_raw.visit_key,
        1 as bp_under_3_ind
    from
        {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
        inner join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
            on stg_encounter_outpatient_raw.visit_key = diagnosis_encounter_all.visit_key
        inner join {{ref('lookup_diagnosis_icd10')}} as lookup_diagnosis_icd10
            on diagnosis_encounter_all.icd10_code = lookup_diagnosis_icd10.icd10_code
    where
        --patient visits for children 3 months and <3 years
        stg_encounter_outpatient_raw.age_years between 0.25 and 2.99999
        and stg_encounter_outpatient_raw.encounter_date between ('01-01-2019') and current_date
        and stg_encounter_outpatient_raw.encounter_type_id in(50, --office visit
                                               101) --appointment
        and stg_encounter_outpatient_raw.appointment_status_id in (2, 6)  --completed, arrived
        -- included encounters
        and ( -- primary care  well visits that were not scheduled as sick visits
            (stg_encounter_outpatient_raw.primary_care_ind = 1
             and stg_encounter_outpatient_raw.well_visit_ind = 1
             and lower(stg_encounter_outpatient_raw.visit_type) not like '%sick%'
            )
            or (--specialty care visits to specific clinics that are "routine"
                stg_encounter_outpatient_raw.specialty_care_ind = 1
                and stg_encounter_outpatient_raw.department_id in (101012023, --KOP Nephrology
                                                101012142, --BGR Nephrology
                                                82377022, --VNJ Nephrology
                                                101022049, --Virtua Nephrology
                                                101022052, --PNJ Nephrology
                                                101012089, -- Brandywine Nephrology
                                                89394025, --Wood NF
                                                84709012 --KOP NF
                )
                and lower(stg_encounter_outpatient_raw.visit_type) not in( 'transplant clinic',
                                                            'dietician',
                                                            'social work',
                                                            'rn only visit',
                                                            'pharmacist',
                                                            'urgent',
                                                            'transplant eval',
                                                            'adol transplant'
                     )--visit types removed at request of team
        ))
        and ((diagnosis_encounter_all.icd10_code in(
                                        'I15.0', 'I15.1', 'I15.2', 'I15.8', 'I15.9', --Secondary Hypertension
                                        'I12.9', --Renal Hypertension
                                        'I70.1', --renal artery sclerosis
                                        'M31.4', --Takayasu arteritis
                                        'N18.9', 'N18.1', 'N18.2', 'N18.3', 'N18.4', 'N18.5', --ckd codes
                                        'P07.31', 'P07.32', 'P07.33', 'P07.34', --preterm infant< 32 weeks
                                        'Q44.7', --Alagille syndrome
                                        'Q64.2', --posterior urethral valves
                                        'Q85.01', --NF1
                                        'Q93.82', --Williams Syndrome
                                        'Z94.0', 'Z94.1', 'Z94.2', 'Z94.4', 'Z94.81'
                                        --solid organ/bone marrow transplant
                                        )
             or lookup_diagnosis_icd10.category in ('N03', --Glomerulonephritis
                                                    'N04', --Nephrotic Syndrome
                                                    'Q24', --congenital heart disease
                                                    'Q96' --Turner Syndrome
                                                    )
             or lookup_diagnosis_icd10.subcategory_1 in ('P07.0', --extremely low birth weight
                                                        'P07.1', --low birth weight
                                                        'P07.2' --extreme preterm baby
                                                        )
             )
            and (diagnosis_encounter_all.problem_list_ind = 1
                 or diagnosis_encounter_all.visit_diagnosis_ind = 1)
        ) --has to be an active diagnosis (for visit team on problem list or as a visit diagnosis
