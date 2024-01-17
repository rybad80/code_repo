select
    /*distinct to each survey received*/
    stg_pfex_duplicate_surveys.visit_key,
    stg_pfex_duplicate_surveys.dept_key,
    /*does not link to any other table; distinct key per row in this table*/
    stg_pfex_duplicate_surveys.survey_key,
    stg_pfex_duplicate_surveys.survey_sent_date,
    stg_pfex_duplicate_surveys.survey_returned_date,
    stg_pfex_duplicate_surveys.cdw_updated_date,
    /*quest_id concatenates a key, survey_line_id, question_id, with | between*/
    /*using survey_line_ind where ch010% moved to pd010% survey line*/
    stg_pfex_duplicate_surveys.survey_line_id,
    /*strip out '?' as etl sometimes add extraneous ones*/
    stg_pfex_duplicate_surveys.section_name_temp,
    /*using section_name with changes shown in stg_pfex_duplicate_surverys*/
    stg_pfex_duplicate_surveys.section_name,
    /*strip out '?' as etl sometimes add extraneous ones*/
    stg_pfex_duplicate_surveys.question_name_temp,
    stg_pfex_duplicate_surveys.question_name,
    /*quest_id concatenates a key, survey_line_id, question_id, with | between*/
    stg_pfex_duplicate_surveys.question_id,
    stg_pfex_duplicate_surveys.comment_text,
    stg_pfex_duplicate_surveys.comment_valence,
    stg_pfex_duplicate_surveys.response_text,
    stg_pfex_duplicate_surveys.tbs_ind,
    /* -2 = comment, 0 = no, 1 = yes*/
    stg_pfex_duplicate_surveys.standard_quest_ind,
    stg_pfex_duplicate_surveys.cahps_ind,
    stg_pfex_duplicate_surveys.cms_ind,
    stg_pfex_duplicate_surveys.comment_ind,
    /*email survey lines end with 'e'*/
    stg_pfex_duplicate_surveys.paper_survey_ind,
    stg_pfex_duplicate_surveys.intl_survey_ind,
    stg_pfex_duplicate_surveys.telehealth_survey_ind,
    /*
    should have 1 survey per encounter, and max 1 response to each
    question on the survey, but the cdw for some reason loads in duplicate
    surveys where everything is the same except for survey_key
    (which links nowhere but is a key for a particular survey and question)
    and cdw_updated_date (which is often a few seconds apart)
    we will de-dup, picking the most recent response
    */

    /*latest cdw update = 1*/
    stg_pfex_duplicate_surveys.record_count,
    stg_pfex_duplicate_surveys.create_by
from
    {{ref('stg_pfex_duplicate_surveys')}} as stg_pfex_duplicate_surveys
where
    stg_pfex_duplicate_surveys.record_count = 1
