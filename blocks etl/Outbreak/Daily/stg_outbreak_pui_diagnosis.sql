{{ config(materialized='table', dist='pat_key') }}

with cohort as (
    select distinct
        stg_outbreak_covid_pui_cohort.pat_key,
        stg_outbreak_covid_pui_cohort.min_specimen_taken_date,
        'covid' as outbreak_type
    from
        {{ref('stg_outbreak_covid_pui_cohort')}} as stg_outbreak_covid_pui_cohort
        union
    select distinct
        stg_outbreak_flu_pui_cohort.pat_key,
        stg_outbreak_flu_pui_cohort.min_specimen_taken_date,
        stg_outbreak_flu_pui_cohort.test_type as outbreak_type
    from
        {{ref('stg_outbreak_flu_pui_cohort')}} as stg_outbreak_flu_pui_cohort
)

select
    cohort.pat_key,
    cohort.outbreak_type,
    max(case when (lower(diagnosis_encounter_all.diagnosis_name) like '%pneumonia%'
            and lower(diagnosis_encounter_all.diagnosis_name) not like '%pneumoniae%'
            and lower(diagnosis_encounter_all.diagnosis_name) not like '%without pneumonia%'
            and lower(diagnosis_encounter_all.diagnosis_name) not like '%vaccin%'
            and lower(diagnosis_encounter_all.diagnosis_name) not like '%family history%')
         then 1 else 0 end) as pneumonia_ind,
    max(case when (lower(diagnosis_encounter_all.diagnosis_name) like '%acute respiratory distress syndrome%'
            and lower(diagnosis_encounter_all.diagnosis_name) not like '%history%'
            and lower(diagnosis_encounter_all.diagnosis_name) not like '%h/o%')
         then 1 else 0 end) as ards_ind,
    max(case when lower(diagnosis_encounter_all.icd10_code) like 'j45%'
            or lower(diagnosis_encounter_all.icd10_code) like 'j43%'
            or lower(diagnosis_encounter_all.icd10_code) like 'j44%'
         then 1 else 0 end) as cld_yn,
    max(case when lower(diagnosis_encounter_all.icd10_code) like '024%'
                     or lower(diagnosis_encounter_all.icd10_code) like 'e08%'
                     or lower(diagnosis_encounter_all.icd10_code) like 'e09%'
                     or lower(diagnosis_encounter_all.icd10_code) like 'e10%'
                     or lower(diagnosis_encounter_all.icd10_code) like 'e11%'
                     or lower(diagnosis_encounter_all.icd10_code) like 'e13%'
         then 1 else 0 end) as diabetes_yn,
    max(case when lower(diagnosis_encounter_all.icd10_code) like 'k07%'
         then 1 else 0 end) as liverdis_yn,
    max(case when (lower(diagnosis_encounter_all.icd10_code) like 'f0%'
                     or lower(diagnosis_encounter_all.icd10_code) like 'f7%'
                     or lower(diagnosis_encounter_all.icd10_code) like 'f84%'
                     or lower(diagnosis_encounter_all.icd10_code) like 'f88%'
                     or lower(diagnosis_encounter_all.icd10_code) like 'f89%')
         then 1 else 0 end) as neuro_yn,
    max(case when complex_chronic_condition_ind = 1
         then 1 else 0 end) as medcond_yn,
    max(case when cvd_ccc_ind = 1
         then 1 else 0 end) as cvd_yn,
    max(case when renal_ccc_ind = 1
         then 1 else 0 end) as renaldis_yn,
    max(case
        when lower(diagnosis_name) like '%chronic%'
        and lower(diagnosis_name) not like '%family history of%' and lower(icd10_code) like 'k7%'
         then 1 else 0 end) as chronic_liver_yn,
    max(case when lower(diagnosis_name) like '%feels%feverish%' then 1 else 0 end) as sfever_yn,
    max(case when lower(icd10_code) = 'r68.83' then 1 else 0 end) as chills_yn,
    max(case when lower(icd10_code) like 'm79%' then 1 else 0 end) as myalgia_yn,
    max(case when lower(diagnosis_name) like '%runny%nose%' then 1 else 0 end) as runnose_yn,
    max(case
        when lower(diagnosis_name) like '%sore%throat%'
            and lower(diagnosis_name) not like '%history%' then 1 else 0 end
    ) as sthroat_yn,
    max(case when lower(icd10_code) like '%r05%' then 1 else 0 end) as cough_yn,
    max(case when lower(icd10_code) like '%r06.02%' then 1 else 0 end) as sob_yn,
    max(case when lower(icd10_code) like 'r11%' then 1 else 0 end) as nauseavomit_yn,
    max(case when lower(icd10_code) like '%r51%' then 1 else 0 end) as headache_yn,
    max(case when lower(icd10_code) like '%r10%' then 1 else 0 end) as abdom_yn,
    max(case when lower(icd10_code) like '%r19.7%' then 1 else 0 end) as diarrhea_yn
from
    cohort
    inner join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on  cohort.pat_key = diagnosis_encounter_all.pat_key
    left join {{ref('diagnosis_medically_complex')}} as diagnosis_medically_complex
        on cohort.pat_key = diagnosis_medically_complex.pat_key
        and diagnosis_medically_complex.encounter_date >= min_specimen_taken_date - interval '30 days'
where
    diagnosis_encounter_all.visit_diagnosis_ind = 1
    and diagnosis_encounter_all.encounter_date between  (min_specimen_taken_date - interval '30 days')
                               and (min_specimen_taken_date + interval '30 days')
group by
    cohort.pat_key,
    cohort.outbreak_type
