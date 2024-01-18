{{ config(meta = {
    'critical': false
}) }}
/* stg_nursing_pfex_w1_metric
create metric rows by pay period for response values & top box score (TBS) booleans
for the Nursing Dashboard Patient & Family Experience sheet
as aligned to department
*/

with
pfex_question_response as (
    select
        pp_end_dt_key as metric_dt_key,
        response_row.nursing_pfex_survey_id,
        response_row.nursing_pfex_question_id,
        response_row.dept_key,
        response_row.score_val,
        response_row.question_mean_value,
        response_row.tbs_ind,
        response_row.denominator, /* to use in Top Box Score only */
        response_row.distinct_count_field
    from
        {{ ref('stg_nursing_pfex_p4_id') }} as response_row
        inner join {{ ref('nursing_pay_period') }} as pp
            on response_row.metric_date between pp.pp_start_dt and pp.pp_end_dt
)

select
    'nQuestionResp' as metric_abbreviation,
    metric_dt_key,
    nursing_pfex_survey_id,
    nursing_pfex_question_id,
    dept_key,
    null as metric_grouper,
    score_val,
    1 as numerator,
    null as denominator,
    1 as row_metric_calculation,
    distinct_count_field
from
    pfex_question_response

union all

select
    'nQuestionTBS' as metric_abbreviation,
    metric_dt_key,
    nursing_pfex_survey_id,
    nursing_pfex_question_id,
    dept_key,
    null as metric_grouper,
    null as score_val,
    tbs_ind as numerator,
    denominator,
    numerator as row_metric_calculation,
    distinct_count_field
from
    pfex_question_response
where
    denominator = 1
