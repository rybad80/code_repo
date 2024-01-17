{{ config(meta = {
    'critical': false
}) }}
/* stg_nursing_pfex_p4_id
set metric row ids and fields/flags for response values & top box score booleans
at date and question granularity
upstream:
P1: non-King of Prussia Hospital Press Ganey results
P2: King of Prussia Hospital Press Ganey results (waiting on this)
P3: dims prep for Survey & Question
nursing_pfex_survey and nursing_pfex_question with specific unique IDs
now:
p4: this to use those IDs to the unioned P1 & P2
*/

with
pfex_question_response as ( /* non-KOPH & KOPH togther */
    select
        response_row.metric_date,
        response_row.survey_line_id,
        response_row.question_id,
        response_row.survey_line_name,
        survey.nursing_pfex_survey_id,
        ques.nursing_pfex_question_id,
        response_row.dept_key,
        response_row.specialty_name,
        response_row.response_text as question_response,
        response_row.mean_value as question_mean_value,
        case
            when response_row.response_text in ('1', '2', '3', '4', '5')
            then case
                when response_row.numerator = 1
                then 1 else 0 end
        end as tbs_ind,
        case
            when response_row.response_text in ('1', '2', '3', '4', '5')
            then 1
        end as denominator, /* to use in Top Box Score only */
        response_row.distinct_count_field
    from
        {{ ref('stg_nursing_pfex_p1_non_koph') }} as response_row /* add KKOPH when ready */
        inner join {{ ref('nursing_pfex_survey') }} as survey
            on response_row.survey_line_id = survey.survey_line_id
            and response_row.survey_line_name = survey.survey_line_name
        inner join {{ ref('nursing_pfex_question') }} as ques
            on survey.nursing_pfex_survey_id = ques.nursing_pfex_survey_id
            and response_row.question_id = ques.pfex_question_id
)

select
    metric_date,
    nursing_pfex_survey_id,
    nursing_pfex_question_id,
    dept_key,
    specialty_name,
    null as metric_grouper,
    question_response as score_val,
    question_mean_value,
    tbs_ind,
    denominator,
    distinct_count_field,
    survey_line_id,
    survey_line_name,
    question_id
from
    pfex_question_response
