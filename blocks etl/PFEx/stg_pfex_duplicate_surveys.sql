select
    /*distinct to each survey received*/
    survey_data.visit_key,
    survey_data.dept_key,
    /*does not link to any other table; distinct key per row in this table*/
    survey_data.survey_key,
    survey_data.survey_sent_dt as survey_sent_date,
    survey_data.survey_return_dt as survey_returned_date,
    survey_data.upd_dt as cdw_updated_date,
    /*quest_id concatenates a key, survey_line_id, question_id, with | between*/
    regexp_replace(master_question.quest_id, '(?:.+\|)(.+)(?:\|.+)', '\1')
    as survey_line_temp,
    /*change old inpatient survey line designator to new survey line*/
    case when lower(survey_line_temp) = 'ch0101u' then 'PD0101'
        when lower(survey_line_temp) = 'ch0101ue' then 'PD0101E'
        when lower(survey_line_temp) = 'ch0102ue' then 'PD0102E'
        else survey_line_temp
    end as survey_line_id,
    /*strip out '?' as etl sometimes add extraneous ones*/
    regexp_replace(master_question.section_nm, '\?+', '')
    as section_name_temp,
    /*statement to change old section names to new section names made by PFEX*/
    case when lower(section_name_temp) = 'nursing care'
            and lower(survey_line_id) like 'pd%' then 'Nurses'
        when lower(section_name_temp) like '%physician'
            and lower(survey_line_id) like 'pd%' then 'Doctors'
        when lower(section_name_temp) = 'nursing'
            and lower(survey_line_id) like 'as%' then 'Nurses'
        when lower(section_name_temp) = 'rehabilitation doctor'
            and lower(survey_line_id) like 'rh%'
            then 'Rehabilitation Doctors'
        when lower(section_name_temp) = 'physician'
            and lower(survey_line_id) like 'iz%' then 'Doctors'
        else initcap(section_name_temp)
    end as section_name,
    /*strip out '?' as etl sometimes add extraneous ones*/
    regexp_replace(master_question.quest_nm, '\?+', '')
    as question_name_temp,
    upper(
        strleft(question_name_temp, 1)) || lower(
        substring(question_name_temp, 2, char_length(question_name_temp))
    ) as question_name,
    /*quest_id concatenates a key, survey_line_id, question_id, with | between*/
    regexp_replace(master_question.quest_id, '(?:.+\|.+\|)(.+)', '\1')
    as question_id,
    survey_data.pat_comment as comment_text,
    survey_data.cmt_rating as comment_valence,
    survey_data.response_text,
    case /*override bad logic in cdw*/
        when (lower(question_id) in ('ch_48', 'cms_23')
            and substr(survey_data.response_text, 1, 2) = '10')
            then 1
        when (lower(question_id) in ('ch_48', 'cms_23')
            and substr(survey_data.response_text, 1, 1) = '5')
            then 0
        when lower(section_name) = 'comments'
            then -2 /*some had been coded as 1*/
        else survey_data.positive_response_ind
    end as tbs_ind,
    case
        when lower(question_id) like 'v7' then 1
        when lower(question_id) like 'm8' then 0
        else master_question.standard_quest_ind /*override bad logic in cdw*/
    end as standard_quest_ind,
    case when lower(question_id) like 'ch_%'
        then 1 else 0
    end as cahps_ind,
    case when lower(question_id) like 'cms_%'
        then 1 else 0
    end as cms_ind,
    case when lower(section_name) = 'comments'
        then 1 else 0
    end as comment_ind,
    /*email survey lines end with 'e'*/
    case when lower(survey_line_id) not like '%e'
        then 1 else 0
    end as paper_survey_ind,
    case when lower(survey_line_id) in ('ch0102ue', 'pd0102e', 'md0103e')
        then 1 else 0
    end as intl_survey_ind,
    case when lower(survey_line_id) in (
        'mt0101ce',
        'mt0102ce',
        'ut0101e',
        'bt0101',
        'bt0101e',
        'ov0101',
        'ov0101e')
        then 1 else 0
    end as telehealth_survey_ind,
    /*
    should have 1 survey per encounter, and max 1 response to each
    question on the survey, but the cdw for some reason loads in
    duplicate surveys where everything is the same except for survey_key
    (which links nowhere but is a key for a particular survey and question)
    and cdw_updated_date (which is often a few seconds apart)
    we will de-dup, picking the most recent response
    */
    row_number() over(
        partition by
            survey_data.visit_key,
            survey_line_id,
            question_id
        order by cdw_updated_date desc
    ) as record_count, -- latest cdw update = 1
    survey_data.create_by
from
    {{source('cdw', 'survey_data')}} as survey_data
inner join {{source('cdw', 'master_question')}} as master_question
    on survey_data.quest_key = master_question.quest_key
where
    lower(survey_data.create_by) = 'pressganey'
    and lower(section_name) not in ('about you', 'demographics')
    and survey_line_id not like '-1'
    and survey_line_id is not null
