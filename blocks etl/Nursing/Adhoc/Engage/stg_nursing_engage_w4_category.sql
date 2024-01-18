/* stg_nursing_engage_w4_category
for matched nurse-cohort/RN excellence dimension/Question/Unit group
combinations between aggregates and rolled up total scores
set each "result against benchmark" for one of the following
categories
  Exceeds = Outperform
  Middle = Met / Just under
  Below
*/
with
match_metrics as (
    select
        aggregate_metric_abbreviation,
        benchmark_metric_abbreviation,
        belowmark_metric_abbreviation,
        rollup_metric_abbreviation,
        category_metric_abbreviation
    from
        {{ ref('nursing_metric_mapping_engage') }}
    where
        benchmark_metric_abbreviation is not null
),

exceeds_match as (
    select
        match_metrics.benchmark_metric_abbreviation,
        score_benchmark.metric_year,
        score_benchmark.rn_dimension_id,
        score_benchmark.nurse_cohort_id,
        score_benchmark.numerator as exceeds_threshold_value
    from
        {{ ref('stg_nursing_engage_w1_benchmark') }} as score_benchmark
        inner join match_metrics
            on score_benchmark.metric_abbreviation
            = match_metrics.benchmark_metric_abbreviation
),

target_match as (
    /* for the exceed threshold get the matching lower threshold for yr, dim, & cohort */
    select
        exceeds_match.benchmark_metric_abbreviation,
        belowmark.metric_year,
        belowmark.rn_dimension_id,
        belowmark.nurse_cohort_id,
        exceeds_match.exceeds_threshold_value,
        belowmark.numerator as below_threshold_value
    from
        {{ ref('stg_nursing_engage_w1_benchmark') }} as belowmark
        inner join match_metrics
            on belowmark.metric_abbreviation
            = match_metrics.belowmark_metric_abbreviation
        inner join exceeds_match
            on match_metrics.benchmark_metric_abbreviation
            = exceeds_match.benchmark_metric_abbreviation
            and belowmark.metric_year = exceeds_match.metric_year
            and belowmark.rn_dimension_id = exceeds_match.rn_dimension_id
            and belowmark.nurse_cohort_id = exceeds_match.nurse_cohort_id
),

engage_w2_and_w3 as (
    select
        metric_abbreviation,
        metric_year,
        rn_dimension_id,
        nurse_cohort_id,
        engagement_question_id,
        unit_group_id,
        metric_grouper,
        numerator,
        denominator,
        row_metric_calculation
    from
        {{ ref('stg_nursing_engage_w2_aggregate') }}

    union all

    select
        metric_abbreviation,
        metric_year,
        rn_dimension_id,
        nurse_cohort_id,
        engagement_question_id,
        unit_group_id,
        metric_grouper,
        numerator,
        denominator,
        row_metric_calculation
    from
        {{ ref('stg_nursing_engage_w3_rollup') }}
),

compare_to_benchmark as (
    select
        category_metric_abbreviation as metric_abbreviation,
        engage_w2_and_w3.metric_year,
        engage_w2_and_w3.rn_dimension_id,
        engage_w2_and_w3.nurse_cohort_id,
        engage_w2_and_w3.engagement_question_id,
        engage_w2_and_w3.unit_group_id,
        engage_w2_and_w3.numerator,
        engage_w2_and_w3.denominator,
        engage_w2_and_w3.numerator
        / engage_w2_and_w3.denominator as row_metric_calculation,
        target_match.exceeds_threshold_value,
        target_match.below_threshold_value,
        case
            when engage_w2_and_w3.row_metric_calculation
                > target_match.exceeds_threshold_value
            then 'exceeds'
            when engage_w2_and_w3.row_metric_calculation
                < target_match.below_threshold_value
            then 'below'
            else 'middle'
            end as result_category
    from
        engage_w2_and_w3
        inner join match_metrics
            on engage_w2_and_w3.metric_abbreviation
            in (match_metrics.rollup_metric_abbreviation,
            match_metrics.aggregate_metric_abbreviation)
        inner join target_match
            on match_metrics.benchmark_metric_abbreviation
            = target_match.benchmark_metric_abbreviation
            and engage_w2_and_w3.metric_year = target_match.metric_year
            and engage_w2_and_w3.rn_dimension_id = target_match.rn_dimension_id
            and engage_w2_and_w3.nurse_cohort_id = target_match.nurse_cohort_id
)

select
    compare_to_benchmark.metric_abbreviation,
    compare_to_benchmark.metric_year,
    compare_to_benchmark.rn_dimension_id,
    compare_to_benchmark.nurse_cohort_id,
    null as job_group_id,
    compare_to_benchmark.engagement_question_id,
    compare_to_benchmark.unit_group_id,
    rslt_catg.dimension_result_category_id,
    null as metric_grouper,
    compare_to_benchmark.numerator,
    compare_to_benchmark.denominator,
    round(compare_to_benchmark.row_metric_calculation, 2) as row_metric_calculation
from
    compare_to_benchmark
    inner join {{ ref('lookup_nursing_survey_dimension_result_category') }} as rslt_catg
        on lower(compare_to_benchmark.result_category)
        = lower(rslt_catg.dimension_result_category_abbreviation)
