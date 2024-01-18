with raw_redcap as (
    select
        master_redcap_project.project_id || '-' || lpad(redcap_detail.record, 8, '0') as survey_id,
        master_redcap_project.project_id,
        master_redcap_project.app_title as project_name,
        redcap_detail.mstr_redcap_event_key,
        redcap_detail.record as record_id,
        master_redcap_question.field_order,
        master_redcap_question.field_nm as field_name,
        master_redcap_question.element_label as question_text,
        master_redcap_element_answr.element_desc,
        redcap_detail.value as answer_value,
        substr(coalesce(
            master_redcap_element_answr.element_desc,
            redcap_detail.value),
            1,
            1000
        ) as answer_text -- label data
    from
        {{ source('cdw', 'redcap_detail') }} as redcap_detail
        inner join {{ source('cdw', 'master_redcap_project') }} as master_redcap_project
            on master_redcap_project.mstr_project_key = redcap_detail.mstr_project_key
        left join {{ source('cdw', 'master_redcap_question') }} as master_redcap_question
            on master_redcap_question.mstr_redcap_quest_key = redcap_detail.mstr_redcap_quest_key
        left join {{ source('cdw', 'master_redcap_element_answr') }} as master_redcap_element_answr
            on master_redcap_element_answr.mstr_redcap_quest_key = redcap_detail.mstr_redcap_quest_key
            and redcap_detail.value = master_redcap_element_answr.element_id
    where
        redcap_detail.cur_rec_ind = 1
        and master_redcap_project.project_id in (924, 982)
    order by
        redcap_detail.record,
        master_redcap_question.field_order
),

redcap_dates as (
    select
        raw_redcap.survey_id,
        max(date_trunc('month', master_redcap_survey_response.survey_response_first_submit_dt)) as visit_date
    from
        raw_redcap
        inner join {{ source('cdw','master_redcap_survey_response') }} as master_redcap_survey_response
            on raw_redcap.mstr_redcap_event_key = master_redcap_survey_response.mstr_redcap_event_key
            and raw_redcap.record_id = master_redcap_survey_response.redcap_record
    where
        master_redcap_survey_response.survey_response_first_submit_dt is not null
    group by
        raw_redcap.survey_id,
        raw_redcap.project_id
),

redcap_answers as (--region
    select
        raw_redcap.survey_id,
        raw_redcap.project_id,
        raw_redcap.record_id as record_id,
        max(
            case when raw_redcap.field_name = 'provider_name' then raw_redcap.answer_text end
        ) as survey_provider_name,
        max(
            case when raw_redcap.field_name like 'provider_e%mail' then raw_redcap.answer_text end
        ) as survey_provider_email,
        max(
            case when raw_redcap.field_name in ('nps', 'recommend_10_scale'
        ) then raw_redcap.answer_value end)::int as recommend_10_scale,
        max(
            case when raw_redcap.field_name in ('met_needs', 'meet_needs_10_scale')
                then raw_redcap.answer_value end
        )::int as met_needs_10_scale,
        max(
            case when raw_redcap.field_name = 'tech_issues' then raw_redcap.answer_text end
        ) as tech_issues_frequency,
        max(
            case when raw_redcap.field_name = 'critical_tech_issues' then raw_redcap.answer_text end
        ) as critical_tech_issues_frequency
    from
        raw_redcap
    group by
        raw_redcap.survey_id,
        raw_redcap.project_id,
        raw_redcap.record_id
)

select
    {{
        dbt_utils.surrogate_key([
            'redcap_answers.survey_id',
            'redcap_answers.project_id',
            'redcap_answers.record_id'
        ])
    }} as survey_key,
    case
        when redcap_answers.project_id = 924 then visit_date -- originally sent every 2 weeks
        when redcap_answers.project_id = 982 then add_months(visit_date, -1) -- sent the month after the visit
        end as visit_date,
    redcap_answers.survey_provider_name,
    redcap_answers.survey_provider_email,
    redcap_answers.recommend_10_scale,
    redcap_answers.met_needs_10_scale,
    case
        when redcap_answers.recommend_10_scale in (9, 10) then 'Promoters'
        when redcap_answers.recommend_10_scale in (7, 8) then 'Passives'
        when redcap_answers.recommend_10_scale < 7 then 'Detractors'
    end as nps_category,
    redcap_answers.tech_issues_frequency,
    redcap_answers.critical_tech_issues_frequency,
    redcap_answers.survey_id,
    redcap_answers.project_id,
    redcap_answers.record_id
from
    redcap_answers
    inner join redcap_dates on redcap_dates.survey_id = redcap_answers.survey_id
where
    redcap_answers.project_id = 982
    or redcap_dates.visit_date >= '2020-04-01'
