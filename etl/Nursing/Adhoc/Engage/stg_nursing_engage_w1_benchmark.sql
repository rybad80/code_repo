/* stg_nursing_engage_w1_benchmark
arrange the national average or CHOP benchmarks into the
nursing metric structure so they are metrics in and of themselves
to display in visualizations but also to be availalbe for the
category wave in order to determine "result against benchmark"
*/
with
national_average as (
    select
        natl_avg.national_average_type,
        natl_avg.survey_year,
        lookup_nursing_survey_dimension.survey_dimension_id,
        lookup_nursing_survey_dimension.survey_dimension_abbreviation,
        lookup_nurse_cohort.nurse_cohort_id,
        natl_avg.national_average,
        natl_avg.threshold_off_national_average,
        natl_avg.national_average as exceeds_threshold,
        natl_avg.national_average
        - natl_avg.threshold_off_national_average as below_threshold_value
    from
        {{ ref('lookup_nursing_survey_dimension_national_average') }} as natl_avg
        left join {{ ref('lookup_nurse_cohort') }} as lookup_nurse_cohort
            on natl_avg.nurse_cohort_abbreviation = lookup_nurse_cohort.nurse_cohort_abbreviation
        left join {{ ref('lookup_nursing_survey_dimension') }} as lookup_nursing_survey_dimension
            on natl_avg.survey_dimension_abbreviation
            = lookup_nursing_survey_dimension.survey_dimension_abbreviation
),

rn_dimension_threshold as (
    select
        'SEbnchScore' as metric_abbreviation,
        survey_year,
        survey_dimension_id,
        survey_dimension_abbreviation,
        nurse_cohort_id,
        exceeds_threshold as numerator
    from
        national_average

    union all

    select
        'SEbelowThreshold' as metric_abbreviation,
        survey_year,
        survey_dimension_id,
        survey_dimension_abbreviation,
        nurse_cohort_id,
        below_threshold_value as numerator
    from
        national_average
),

question_threshold as (
    select
        metric_abbreviation || 'Q' as metric_abbreviation,
        survey_year,
        survey_dimension_id,
        survey_question_id,
        nurse_cohort_id,
        numerator
    from
        rn_dimension_threshold
        left join {{ ref('lookup_nursing_survey_dimension_nursing_question') }}
            as lookup_nursing_survey_dimension_nursing_question
            on rn_dimension_threshold.survey_dimension_abbreviation
            = lookup_nursing_survey_dimension_nursing_question.survey_dimension_abbreviation
)

select
    metric_abbreviation,
    survey_year as metric_year,
    survey_dimension_id as rn_dimension_id,
    nurse_cohort_id,
    null as job_group_id,
    null as engagement_question_id,
    null as unit_group_id,
    null as dimension_result_category_id,
    null as metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    rn_dimension_threshold

union all

select
    metric_abbreviation,
    survey_year as metric_year,
    survey_dimension_id as rn_dimension_id,
    nurse_cohort_id,
    null as job_group_id,
    survey_question_id as engagement_question_id,
    null as unit_group_id,
    null as dimension_result_category_id,
    null as metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    question_threshold
