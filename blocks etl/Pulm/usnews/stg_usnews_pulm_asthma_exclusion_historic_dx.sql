-- RUNTIME_S   QH_ESTCOST  LOG_COST    QH_SNIPPETS QH_ESTMEM   N_CHAR
-- 00:00:06    140456      5.147540    4           8031        4901

with cohort as (
select
    mrn,
    patient_name,
    pat_key
from {{ref('stg_usnews_pulm_asthma_base')}}
where primary_dx_ind = 1
group by
    mrn,
    patient_name,
    pat_key
)
/* exclusion cohort based on historical dx
 *
 * neuromuscular disease: based on s_cdw_usnwr_pulm_nmd
 * rare lung disease: based on s_cdw_usnwr_pulm_enc
 * tracheostomy: based on historical diagnosis with diagnosis name of 'Tracheostomy status'
 * sickle cell disease: based on historical diagnosis code
 * hie: patients with any historical diagnosis ICD-10 code of 'p91%' or 'f88%' or 'g93.4%'
 * cerebral palsy: based on discussions with CP team
 * congenital heart disease: patients with any historical diagnosis ICD-10 code of 'q21%' or 'q24%'
 * other: manual exclusion per Joe's review
 * */
/*
 * note: unique patient count in new codes can be different from the old codes,
 * even though the logic is correct
 * this is because the case when statement would only assign 1 exclusion cohort
 * to a patient, even when the patient belongs to multiple exclusion cohorts
 * this would not impact much the final output since all of these patients would
 * be excluded as long as they belong to an exclusion cohort
 */
/* validated as of 2/21 */
select
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    historic_dx.encounter_date,
        case
            when historic_code_list.code is not null and lower(historic_code_list.question_number) like 'j29%'
            then 'nmd'
            when historic_code_list.code is not null and lower(historic_code_list.question_number) like 'j26%'
            then 'rare lung disease'
            when lower(historic_dx.diagnosis_name) like '%tracheostomy status%'
                and historic_dx.source_summary not like '%pb_transaction%'
            then 'trache'
            when lower(historic_dx.icd10_code) like 'd57.%'
            then 'sickle cell'
            when (lower(historic_dx.icd10_code) like 'p91%'
                    or lower(historic_dx.icd10_code) like 'f88%'
                    or lower(historic_dx.icd10_code) like 'g93.4%')
                and historic_dx.source_summary not like '%pb_transaction%'
            then 'hie'
            when lower(historic_dx.icd10_code) in ('g80.0', 'g80.1', 'g80.2', 'g80.3', 'g80.4', 'g80.8', 'g80.9')
                and historic_dx.source_summary like '%problem_list%'
            then 'cp'
            when (lower(historic_dx.icd10_code) like 'q21%'
                    or lower(historic_dx.icd10_code) like 'q24%')
                and historic_dx.source_summary not like '%pb_transaction%'
            then 'congenital heart'
            when historic_dx.icd10_code in ('P27.1', -- bronchopulmonary dysplasia originating in the perinatal period --noqa: L016
                                            'Q33.6', -- congenital hypoplasia and dysplasia of lung
                                            'Q32.4', -- other congenital malformations of bronchus
                                            'Q79.0', -- congenital diaphragmatic hernia
                                            'P07.15', -- other low birth weight newborn, 1250-1499 grams
                                            'D80.1', -- nonfamilial hypogammaglobulinemia
                                            'D80.8', -- other immunodeficiencies with predominantly antibody defects --noqa: L016
                                            'I42.2', -- other hypertrophic cardiomyopathy
                                            'M32.9', -- systemic lupus erythematosus, unspecified
                                            'P07.35', -- preterm newborn, gestational age 32 completed weeks
                                            'D84.9', -- immunodeficiency, unspecified
                                            'Q90.9', -- down syndrome, unspecified
                                            'D80.4', -- selective deficiency of immunoglobulin m [igm]
                                            'D82.8', -- immunodeficiency associated with other specified major defects --noqa: L016
                                            'P07.32', -- preterm newborn, gestational age 29 completed weeks
                                            'J47.9', -- bronchiectasis, uncomplicated
                                            'P07.22', -- extreme immaturity of newborn, gestational age 23 completed weeks --noqa: L016
                                            'P07.02', -- extremely low birth weight newborn, 500-749 grams
                                            'Z87.74', -- personal history of (corrected) congenital malformations of heart and circulatory system --noqa: L016
                                            'J94.0', -- chylous effusion
                                            'Q32.0', -- congenital tracheomalacia
                                            'I26.99', -- other pulmonary embolism without acute cor pulmonale
                                            'G35', -- multiple sclerosis
                                            'D83.9', -- common variable immunodeficiency, unspecified
                                            'Q32.2', -- congenital bronchomalacia
                                            'D89.40', -- mast cell activation, unspecified
                                            'M08.3', -- juvenile rheumatoid polyarthritis (seronegative)
                                            'M35.81', -- multisystem inflammatory syndrome
                                            'J98.2', -- interstitial emphysema
                                            'P07.31', -- preterm newborn, gestational age 28 completed weeks
                                            'Z86.711', -- personal history of pulmonary embolism
                                            'I42.0' -- dilated cardiomyopathy
                                            ) -- manually reviewed by Dr. Piccione - 12/6/22
                and historic_dx.visit_diagnosis_ind = 1
            then 'other'
            end as exc_cohort,
        case
            when exc_cohort = 'cp'
            then 0
            else 1
            end as date_cutoff_ind
from cohort
inner join {{ref('diagnosis_encounter_all')}} as historic_dx
    on cohort.pat_key = historic_dx.pat_key
left join {{ref('usnews_code_list')}} as historic_code_list
    on historic_dx.icd10_code = historic_code_list.code
    and lower(historic_code_list.division) like '%pulm%'
    and (lower(historic_code_list.question_number) like 'j29%'
        or lower(historic_code_list.question_number) like 'j26%')
    and lower(historic_code_list.code_type) in ('icd10_code')
    and historic_code_list.submission_end_year is null
where exc_cohort is not null
group by
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    historic_dx.encounter_date,
    exc_cohort,
    date_cutoff_ind
