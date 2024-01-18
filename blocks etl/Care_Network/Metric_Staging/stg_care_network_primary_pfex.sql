select
    pfex_all.visit_key,
    case
        when pfex_all.question_id in ('O15') then 'Staff Worked Together Top Box Score'
        when pfex_all.question_id in ('O4') then 'Likelihood To Recommend Top Box Score'
        when pfex_all.question_id in ('V7') then 'Informed About Delays Top Box Score'
        when pfex_all.question_id in ('A1') then 'Ease of Scheduling Top Box Score'
    else 'Metric is not ready for use'
    end as question_metric_name,
    case
        when pfex_all.question_id in ('O15') then 'pc_pfex_staff'
        when pfex_all.question_id in ('O4') then 'pc_pfex_recommend'
        when pfex_all.question_id in ('V7') then 'pc_pfex_delays'
        when pfex_all.question_id in ('A1') then 'pc_pfex_scheduling'
    else 'Metric is not ready for use'
    end as question_metric_id,
    case
        when lower(pfex_all.section_name ) = 'access' then 'Access Top Box Score'
    else 'Metric is not ready for use'
    end as section_metric_name,
    case
        when lower(pfex_all.section_name ) = 'access' then 'pc_pfex_access'
    else 'Metric is not ready for use'
    end as section_metric_id,
    case
        when lower(pfex_all.survey_line_name) = 'primary care' then 'Overall Primary Care Top Box Score'
    else 'Metric is not ready for use'
    end as survey_line_metric_name,
    case
        when lower(pfex_all.survey_line_name) = 'primary care' then 'pc_pfex_overall'
    else 'Metric is not ready for use'
    end as survey_line_metric_id,
    pfex_all.department_name as drill_down,
    pfex_all.provider_name,
    pfex_all.visit_date,
    pfex_all.survey_sent_date,
    pfex_all.survey_returned_date,
    pfex_all.specialty_name,
    pfex_all.department_name,
    pfex_all.department_id,
    pfex_all.survey_line_name,
    pfex_all.survey_line_id,
    pfex_all.section_name,
    pfex_all.question_name,
    pfex_all.question_id,
    pfex_all.response_text,
    pfex_all.tbs_ind,
    pfex_all.survey_key,
    pfex_all.pat_key,
    pfex_all.dept_key

from
    {{ ref('pfex_all') }} as pfex_all

where
    lower(pfex_all.survey_line_name) = 'primary care'
    and lower(pfex_all.question_id) != 'floating' and lower(pfex_all.question_id) not like 'sect%'
