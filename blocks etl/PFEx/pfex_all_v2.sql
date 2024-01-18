with combine_tables as (
     select
          stg_pfex_demo_meta.survey_id,
          stg_pfex_demo_meta.campus,
          stg_pfex_demo_meta.client_id,
          stg_pfex_demo_meta.patient_name,
          stg_pfex_demo_meta.mrn,
          stg_pfex_demo_meta.dob,
          stg_pfex_demo_meta.csn,
          stg_pfex_demo_meta.encounter_date,
          stg_pfex_demo_meta.preferred_name,
          stg_pfex_demo_meta.epic_language,
          stg_pfex_demo_meta.epic_race_ethnicity,
          stg_pfex_demo_meta.pg_language,
          stg_pfex_demo_meta.pg_race_ethnicity,
          stg_pfex_demo_meta.provider_name,
          stg_pfex_demo_meta.provider_id,
          stg_pfex_demo_meta.visit_date,
          stg_pfex_demo_meta.hospital_admit_date,
          stg_pfex_demo_meta.hospital_discharge_date,
          stg_pfex_demo_meta.survey_returned_date,
          current_timestamp as cdw_updated_date,
          stg_pfex_demo_meta.specialty_name,
          stg_pfex_demo_meta.department_name,
          stg_pfex_demo_meta.department_id,
          stg_pfex_line_name_lookup.survey_line_name,
          stg_pfex_demo_meta.survey_line_id,
          stg_pfex_questkey.section_name,
          stg_pfex_demo_meta.service,
          stg_pfex_questkey.question_name,
          stg_pfex_all_responses.question_id,
          stg_pfex_all_responses.comment_text,
          stg_pfex_all_responses.comment_valence,
          stg_pfex_all_responses.response_type,
          stg_pfex_all_responses.response_text,
          stg_pfex_all_responses.tbs_ind,
          stg_pfex_all_responses.mean_value,
          stg_pfex_all_responses.updated,
          stg_pfex_questkey.standard_question_ind,
          stg_pfex_all_responses.cahps_ind,
          stg_pfex_all_responses.cms_ind,
          stg_pfex_all_responses.comment_ind,
          stg_pfex_line_name_lookup.paper_survey_ind,
          stg_pfex_line_name_lookup.intl_survey_ind,
          stg_pfex_line_name_lookup.telehealth_survey_ind,
          stg_pfex_demo_meta.provider_key,
          stg_pfex_demo_meta.department_key,
          stg_pfex_demo_meta.patient_key,
          stg_pfex_demo_meta.hospital_account_key,
          stg_pfex_demo_meta.encounter_key,
          stg_pfex_demo_meta.create_by
     from
          {{ref('stg_pfex_all_responses')}} as stg_pfex_all_responses
     inner join {{ref('stg_pfex_questkey')}} as stg_pfex_questkey
          on stg_pfex_all_responses.service = stg_pfex_questkey.service
          and stg_pfex_all_responses.question_id = stg_pfex_questkey.question_id
     inner join
          {{ref('stg_pfex_demo_meta')}} as stg_pfex_demo_meta
          on stg_pfex_all_responses.survey_id = stg_pfex_demo_meta.survey_id
     inner join
          {{ref('stg_pfex_line_name_lookup')}} as stg_pfex_line_name_lookup
          on stg_pfex_demo_meta.survey_line_id = stg_pfex_line_name_lookup.survey_line_id
          and stg_pfex_demo_meta.department_id = stg_pfex_line_name_lookup.department_id
)
select
     {{
               dbt_utils.surrogate_key([
               'survey_id',
               'question_id',
               'encounter_key'
               ])
     }} as survey_key,
     combine_tables.*
     from
          combine_tables
        
