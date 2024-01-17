/*  stg_nursing_engage_w3_rollup
rollup questions or unit_groups to applicable higher level
in order to capture "result against benchmark"
for various combinations such as questions overall,
questions into RN excellence dimension, or
unit_groups into their cost center group or type of Acute unit,
such as ICU or Medical or to KOP and Phila
*/
with
match_rollup_metric as (
    select
        metric_focus,
        aggregate_metric_abbreviation,
        rollup_metric_abbreviation,
        grouper_type
    from
        {{ ref('nursing_metric_mapping_engage') }}
    where
        rollup_metric_abbreviation is not null
),

aggregate_nums as (
    select
        metric_abbreviation,
        metric_year,
        rn_dimension_id,
        engagement_question_id,
        nurse_cohort_id,
        unit_group_id,
        numerator,
        denominator,
        row_metric_calculation
    from
        {{ ref('stg_nursing_engage_w2_aggregate') }}
),

rn_dim_rollup as (
    select
        match_rollup_metric.rollup_metric_abbreviation as metric_abbreviation,
        aggregate_source_score.metric_year,
        aggregate_source_score.rn_dimension_id,
        aggregate_source_score.nurse_cohort_id,
        aggregate_source_score.unit_group_id,
        sum(aggregate_source_score.numerator) as numerator,
        sum(aggregate_source_score.denominator) as denominator,
        sum(aggregate_source_score.numerator) / sum(aggregate_source_score.denominator) as row_metric_calculation
    from
        aggregate_nums as aggregate_source_score
        inner join match_rollup_metric
            on aggregate_source_score.metric_abbreviation
			= match_rollup_metric.aggregate_metric_abbreviation
    where
        match_rollup_metric.metric_focus = 'RNdim'
    group by
        match_rollup_metric.rollup_metric_abbreviation,
        aggregate_source_score.metric_year,
        aggregate_source_score.rn_dimension_id,
        aggregate_source_score.nurse_cohort_id,
        aggregate_source_score.unit_group_id
),

non_score_rollups as (
select
    match_rollup_metric.rollup_metric_abbreviation as metric_abbreviation,
    aggregate_nums.metric_year,
    aggregate_nums.rn_dimension_id,
    null::integer as engagement_question_id,
    aggregate_nums.nurse_cohort_id,
    null::integer as unit_group_id,
    match_rollup_metric.grouper_type as metric_grouper,
    sum(aggregate_nums.numerator) as numerator,
    sum(aggregate_nums.denominator) as denominator,
    sum(aggregate_nums.numerator) / sum(aggregate_nums.denominator) as row_metric_calculation
from
    aggregate_nums
    inner join match_rollup_metric
        on aggregate_nums.metric_abbreviation
        = match_rollup_metric.aggregate_metric_abbreviation
where
    match_rollup_metric.metric_focus != 'RNdim'
    and match_rollup_metric.grouper_type = 'RN Exc Dim' /* RN Excellence dimension */
group by
    match_rollup_metric.rollup_metric_abbreviation,
    aggregate_nums.metric_year,
    aggregate_nums.rn_dimension_id,
    aggregate_nums.nurse_cohort_id,
    match_rollup_metric.grouper_type

union all

select
    match_rollup_metric.rollup_metric_abbreviation as metric_abbreviation,
    aggregate_nums.metric_year,
    null::integer as rn_dimension_id,
    null::integer as engagement_question_id,
    aggregate_nums.nurse_cohort_id,
    aggregate_nums.unit_group_id,
    match_rollup_metric.grouper_type as metric_grouper,
    sum(aggregate_nums.numerator) as numerator,
    sum(aggregate_nums.denominator) as denominator,
    sum(aggregate_nums.numerator) / sum(aggregate_nums.denominator) as row_metric_calculation
from
    aggregate_nums
    inner join match_rollup_metric
        on aggregate_nums.metric_abbreviation
        = match_rollup_metric.aggregate_metric_abbreviation
where
    match_rollup_metric.metric_focus != 'RNdim'
    and match_rollup_metric.grouper_type = 'RNs Unit' /* Unit */
group by
    match_rollup_metric.rollup_metric_abbreviation,
    aggregate_nums.metric_year,
    aggregate_nums.nurse_cohort_id,
    aggregate_nums.unit_group_id,
    match_rollup_metric.grouper_type

union all

select
    rollup_metric_abbreviation as metric_abbreviation,
    aggregate_nums.metric_year,
    null::integer as rn_dimension_id,
    aggregate_nums.engagement_question_id,
    aggregate_nums.nurse_cohort_id,
    null::integer as unit_group_id,
    match_rollup_metric.grouper_type as metric_grouper,
    sum(aggregate_nums.numerator) as numerator,
    sum(aggregate_nums.denominator) as denominator,
    sum(aggregate_nums.numerator) / sum(aggregate_nums.denominator) as row_metric_calculation
from
    aggregate_nums
    inner join match_rollup_metric
        on aggregate_nums.metric_abbreviation
        = match_rollup_metric.aggregate_metric_abbreviation
where
    match_rollup_metric.metric_focus != 'RNdim'
    and match_rollup_metric.grouper_type = 'RNs Ques' /* Question */
group by
    match_rollup_metric.rollup_metric_abbreviation,
    aggregate_nums.metric_year,
    aggregate_nums.nurse_cohort_id,
    aggregate_nums.engagement_question_id,
    match_rollup_metric.grouper_type
)

select
    metric_abbreviation,
    metric_year,
    rn_dimension_id,
    nurse_cohort_id,
    null as job_group_id,
    null as engagement_question_id,
    unit_group_id,
    null as dimension_result_category_id,
    null as metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
    rn_dim_rollup /* of score */

union all

select
    metric_abbreviation,
    metric_year,
    rn_dimension_id,
    nurse_cohort_id,
    null as job_group_id,
    engagement_question_id,
    unit_group_id,
    null as dimension_result_category_id,
    metric_grouper,
    numerator,
    denominator,
    row_metric_calculation
from
    non_score_rollups /* of % favorable, neutral or unfavorable */
