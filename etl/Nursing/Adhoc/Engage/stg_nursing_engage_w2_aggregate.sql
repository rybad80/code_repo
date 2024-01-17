/* stg_nursing_engage_w2_aggregate
arrange the nursing only scores, top box and favorable/neutral/unfavorable
counts numerators with the denominator of number of respondents
aggregates at nurse cohort and unit group levels
into the nursing metric structure
Nurse cohorts are:
    All RNs
    Direct Care RN
    Nurse Mgr
    <50% Care RN
*/
with
nursing_survey_aggregate as (
    select /* nursing_survey_unit_question_aggregates
        for each RN subset cohort */
        survey_year,
        survey_dimension_id,
        survey_question_id,
        unit_group_id,
        nurse_cohort_id,
        total_score,
        num_respondents,
        top_box_count,
        favorable_count,
        neutral_count,
        unfavorable_count
    from
        {{ ref('stg_nursing_engage_p2_question_aggregate') }}
    where
        num_respondents >= 5 -- expected in data source but being robust

    union all

    select /* nursing_survey_unit_question_aggregates "All RNs" cohort is
        the three cohorts rolled together: patient care + management + <50% patient care */
        survey_year,
        survey_dimension_id,
        survey_question_id,
        unit_group_id,
        5 as nurse_cohort_id, /* All RNs */
        sum(total_score),
        sum(num_respondents),
        sum(top_box_count),
        sum(favorable_count),
        sum(neutral_count),
        sum(unfavorable_count)
    from
        {{ ref('stg_nursing_engage_p2_question_aggregate') }}
    where
        num_respondents >= 5
group by
        survey_year,
        survey_dimension_id,
        survey_question_id,
        unit_group_id
),

union_question_metrics as (
    select
        'SEquesScore' as metric_abbreviation,
        survey_year,
        survey_question_id,
        survey_dimension_id,
        unit_group_id,
        nurse_cohort_id,
        sum(total_score) as numerator,
        sum(num_respondents) as denominator,
        sum(total_score) / sum(num_respondents) as row_metric_calculation
    from
        nursing_survey_aggregate
    group by
        survey_year,
        survey_question_id,
        survey_dimension_id,
        unit_group_id,
        nurse_cohort_id

    union all

    select
        'SEquesTopBox' as metric_abbreviation,
        survey_year,
        survey_question_id,
        survey_dimension_id,
        unit_group_id,
        nurse_cohort_id,
        sum(top_box_count) as numerator,
        sum(num_respondents) as denominator,
        sum(top_box_count) / sum(num_respondents) as row_metric_calculation
    from
        nursing_survey_aggregate
    group by
        survey_year,
        survey_question_id,
        survey_dimension_id,
        unit_group_id,
        nurse_cohort_id

    union all

    select
        'SEquesFav' as metric_abbreviation,
        survey_year,
        survey_question_id,
        survey_dimension_id,
        unit_group_id,
        nurse_cohort_id,
        sum(favorable_count) as numerator,
        sum(num_respondents) as denominator,
        sum(favorable_count) / sum(num_respondents) as row_metric_calculation
    from
        nursing_survey_aggregate
    group by
        survey_year,
        survey_question_id,
        survey_dimension_id,
        unit_group_id,
        nurse_cohort_id

    union all

    select
        'SEquesNeut' as metric_abbreviation,
        survey_year,
        survey_question_id,
        survey_dimension_id,
        unit_group_id,
        nurse_cohort_id,
        sum(neutral_count) as numerator,
        sum(num_respondents) as denominator,
        sum(neutral_count) / sum(num_respondents) as row_metric_calculation
    from
        nursing_survey_aggregate
    group by
        survey_year,
        survey_question_id,
        survey_dimension_id,
        unit_group_id,
        nurse_cohort_id

    union all

    select
        'SEquesUnfav' as metric_abbreviation,
        survey_year,
        survey_question_id,
        survey_dimension_id,
        unit_group_id,
        nurse_cohort_id,
        sum(unfavorable_count) as numerator,
        sum(num_respondents) as denominator,
        sum(unfavorable_count) / sum(num_respondents) as row_metric_calculation
    from
        nursing_survey_aggregate
    group by
        survey_year,
        survey_question_id,
        survey_dimension_id,
        unit_group_id,
        nurse_cohort_id
)

select
    metric_abbreviation,
    survey_year as metric_year,
    survey_dimension_id as rn_dimension_id,
    nurse_cohort_id,
    null as job_group_id,
    survey_question_id as engagement_question_id,
    unit_group_id,
    null as dimension_result_category_id,
    null as metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
    union_question_metrics
