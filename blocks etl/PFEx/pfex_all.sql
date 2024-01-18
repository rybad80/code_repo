select
    /*does not link to any other table; distinct key per row in this table*/
    stg_pfex_deduplicate.survey_key,
    stg_patient_ods.patient_name,
    stg_patient_ods.mrn,
    stg_patient_ods.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_patient_ods.preferred_language,
    stg_patient_ods.preferred_name,
    /*provider is whats on survey; not same as stg_encounter.provider_name*/
    stg_encounter.provider_name,
    stg_encounter.provider_id,
    stg_pfex_survey_names_dates.visit_date,
    stg_pfex_deduplicate.survey_sent_date,
    stg_pfex_deduplicate.survey_returned_date,
    stg_pfex_deduplicate.cdw_updated_date,
    case
            when stg_encounter.department_name = 'WOOD ANES PAIN MGMT' then 'WOOD ANES PAIN MGMT'
            when stg_encounter.department_name = 'MKT 3550 SPC IMM FAM CARE' then 'GENERAL PEDIATRICS'
            when stg_encounter.specialty_name = 'HEMATOLOGY ONCOLOGY' then 'ONCOLOGY'
            when stg_encounter.specialty_name = 'RADIATION ONCOLOGY' then 'ONCOLOGY'
            when stg_encounter.specialty_name = 'NEONATAL FOLLOWUP' then 'NEONATOLOGY'
            when lower(stg_encounter.specialty_name) = 'leukodystropy' then 'NEUROLOGY'
            when lower(stg_encounter.specialty_name) = 'dentistry' then 'PLASTIC SURGERY'
            when lower(stg_encounter.specialty_name) = 'pediatric general thoracic surgery' then 'GENERAL SURGERY'
            when lower(stg_encounter.department_name) like '%feeding%'
                and lower(stg_encounter.department_name) not like '%bh%' then 'FEEDING'
            else stg_encounter.specialty_name
        end as specialty_name,
    case
        when lower(stg_encounter.department_name) = '5 east'
            and stg_pfex_survey_names_dates.visit_date > '2020-08-09'
            then '5 EAST ITCU'
        when lower(stg_encounter.department_name) = '5 east'
            and stg_pfex_survey_names_dates.visit_date <= '2020-08-09'
            then '11 NORTHWEST'
        when lower(stg_encounter.department_name) = '3eastcsh'
            and stg_pfex_survey_names_dates.visit_date >= '2020-12-01'
            then '3ECSH ADOL'
        else stg_encounter.department_name
    end as department_name,
    cast(stg_encounter.department_id as bigint) as department_id,
    stg_pfex_survey_names_dates.survey_line_name,
    /*changes to inpatient survey line found in stg_pfex_duplicate_surveys*/
    stg_pfex_deduplicate.survey_line_id,
    /*changes to a few section names found in stg_pfex_duplicate_surveys*/
    stg_pfex_deduplicate.section_name,
    stg_pfex_survey_questions.question_name,
    stg_pfex_deduplicate.question_id,
    stg_pfex_deduplicate.comment_text,
    stg_pfex_deduplicate.comment_valence,
    response_type.response_type,
    stg_pfex_deduplicate.response_text,
    stg_pfex_deduplicate.tbs_ind,
    case /*report on a 0-5 scale*/
        when response_type.response_type = '0 to 10'
            and stg_pfex_deduplicate.response_text not like '0%'
            and stg_pfex_deduplicate.response_text not like '10%'
            then cast(stg_pfex_deduplicate.response_text as numeric) * 0.5
        when response_type.response_type = '0 to 10'
            and stg_pfex_deduplicate.response_text like '0%' then 0
        when response_type.response_type = '0 to 10'
            and stg_pfex_deduplicate.response_text like '10%' then 5
        when response_type.response_type = '1 to 5'
            then (cast(stg_pfex_deduplicate.response_text as numeric)
                - 1) * (5.0 / 4)
        when lower(
            response_type.response_type) = 'never-sometimes-usually-always'
            and lower(stg_pfex_deduplicate.response_text) = 'never'
            then 0
        when lower(
            response_type.response_type) = 'never-sometimes-usually-always'
            and lower(stg_pfex_deduplicate.response_text) = 'sometimes'
            then 5.0 / 3
        when lower(
            response_type.response_type) = 'never-sometimes-usually-always'
            and lower(stg_pfex_deduplicate.response_text) = 'usually'
            then 10.0 / 3
        when lower(
            response_type.response_type) = 'never-sometimes-usually-always'
            and lower(stg_pfex_deduplicate.response_text) = 'always'
            then 5
        when lower(response_type.response_type)
            = 'definitely no-probably no-probably yes-definitely yes'
            and lower(stg_pfex_deduplicate.response_text) = 'definitely no'
            then 0
        when lower(response_type.response_type)
            = 'definitely no-probably no-probably yes-definitely yes'
            and lower(stg_pfex_deduplicate.response_text) = 'probably no'
            then 5.0 / 3
        when lower(response_type.response_type)
            = 'definitely no-probably no-probably yes-definitely yes'
            and lower(stg_pfex_deduplicate.response_text) = 'probably yes'
            then 10.0 / 3
        when lower(response_type.response_type)
            = 'definitely no-probably no-probably yes-definitely yes'
            and lower(stg_pfex_deduplicate.response_text) = 'definitely yes'
            then 5
        when lower(response_type.response_type)
            = 'strongly disagree-disagree-agree-strongly agree'
            and lower(stg_pfex_deduplicate.response_text) = 'strongly disagree'
            then 0
        when lower(response_type.response_type)
            = 'strongly disagree-disagree-agree-strongly agree'
            and lower(stg_pfex_deduplicate.response_text) = 'disagree'
            then 5.0 / 3
        when lower(response_type.response_type)
            = 'strongly disagree-disagree-agree-strongly agree'
            and lower(stg_pfex_deduplicate.response_text) = 'agree'
            then 10.0 / 3
        when lower(response_type.response_type)
            = 'strongly disagree-disagree-agree-strongly agree'
            and lower(stg_pfex_deduplicate.response_text) = 'strongly agree'
            then 5
        when lower(response_type.response_type)
            = 'no-yes, somewhat-yes, definitely'
            and lower(stg_pfex_deduplicate.response_text) = 'no'
            then 0
        when lower(response_type.response_type)
            = 'no-yes, somewhat-yes, definitely'
            and lower(stg_pfex_deduplicate.response_text) = 'yes, somewhat'
            then 2.5
        when lower(
            response_type.response_type) = 'no-yes, somewhat-yes, definitely'
            and lower(stg_pfex_deduplicate.response_text) = 'yes, definitely'
            then 5
        when lower(response_type.response_type) in (
            'no-yes',
            'unchecked-checked',
            'another facility-another home-own home',
            'comment')
            then null
        else null
    end as mean_value,
    stg_pfex_deduplicate.standard_quest_ind,
    stg_pfex_deduplicate.cahps_ind,
    stg_pfex_deduplicate.cms_ind,
    stg_pfex_deduplicate.comment_ind,
    stg_pfex_deduplicate.paper_survey_ind,
    stg_pfex_deduplicate.intl_survey_ind,
    stg_pfex_deduplicate.telehealth_survey_ind,
    stg_encounter.visit_prov_key as prov_key,
    -- provider.prov_key, /*not necessarily the same as stg_encounter.prov_key*/
    stg_pfex_deduplicate.dept_key,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    /*visit_key distinct to each survey received*/
    stg_encounter.visit_key,
    stg_pfex_deduplicate.create_by
from
    {{ref('stg_pfex_deduplicate')}} as stg_pfex_deduplicate
inner join {{ref('stg_pfex_response_type')}} as response_type
    on stg_pfex_deduplicate.question_id = response_type.question_id
inner join {{ref('stg_pfex_survey_questions')}} as stg_pfex_survey_questions
    on stg_pfex_survey_questions.survey_key = stg_pfex_deduplicate.survey_key
-- left join {{source('cdw', 'department')}} as department
--     on stg_pfex_deduplicate.dept_key = department.dept_key
left join {{ref('stg_pfex_survey_names_dates')}} as stg_pfex_survey_names_dates
    on stg_pfex_survey_names_dates.survey_key = stg_pfex_deduplicate.survey_key
left join {{ref('stg_encounter')}} as stg_encounter
    on stg_pfex_deduplicate.visit_key = stg_encounter.visit_key
left join {{ref('stg_patient_ods')}} as stg_patient_ods
    on stg_encounter.patient_key = stg_patient_ods.patient_key
left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
    on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
-- left join {{source('cdw', 'visit')}} as visit
--     on stg_pfex_deduplicate.visit_key = visit.visit_key
-- left join {{source('cdw', 'provider')}} as provider
--     on stg_encounter.visit_prov_key = provider.prov_key
where
    /*removing survey line variables from name column*/
    stg_pfex_survey_names_dates.survey_line_name not in(
        'md0101', 'md0101e', 'md0103e', 'mt0101ce'
    )  
    and {{ limit_dates_for_dev(ref_date = 'stg_encounter.encounter_date') }}
 