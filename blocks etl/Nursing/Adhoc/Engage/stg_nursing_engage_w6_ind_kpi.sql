/* stg_nursing_engage_w6_ind_kpi
set the top 10 or XXX in various categories
and derive KPI(s) for nursing engagement overall
*/
with gather_dim_score as (
    select
        rollup_score.metric_year,
        rollup_score.unit_group_id,
        rollup_score.rn_dimension_id,
        rollup_score.nurse_cohort_id,
        round(rollup_score.row_metric_calculation, 2) as score_to_rank,
        lookup_nursing_unit_group.unit_group_type
    from
        {{ ref('stg_nursing_engage_w3_rollup') }}	as rollup_score
        inner join {{ ref('lookup_nursing_unit_group') }} as lookup_nursing_unit_group
            on rollup_score.unit_group_id = lookup_nursing_unit_group.unit_group_id
    where
        rollup_score.metric_abbreviation = 'SErollupRNdim'
        and rollup_score.unit_group_id is not null
        and rollup_score.nurse_cohort_id in (5, 10) -- unit group Patient Care RN avg scores and All RNs
),

rank_the_score as (
    select
        metric_year,
        unit_group_id,
        rn_dimension_id,
        nurse_cohort_id,
        score_to_rank,
        unit_group_type,
        dense_rank() over (
            partition by
                metric_year,
                rn_dimension_id,
                nurse_cohort_id
            order by score_to_rank desc
            ) as all_score_rank,
        dense_rank() over (
            partition by
                metric_year,
                rn_dimension_id,
                nurse_cohort_id,
                unit_group_type
            order by score_to_rank desc
            ) as type_score_rank
    from
        gather_dim_score
)

select
    'RNdimTop10all' as metric_abbreviation,
    metric_year,
    rn_dimension_id,
    nurse_cohort_id,
    null as job_group_id,
    null as engagement_question_id,
    unit_group_id,
    null as dimension_result_category_id,
    null as metric_grouper,
    score_to_rank as numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    rank_the_score
where
    all_score_rank <= 10

union all

select
    'RNdimTop7amb' as metric_abbreviation,
    metric_year,
    rn_dimension_id,
    nurse_cohort_id,
    null as job_group_id,
    null as engagement_question_id,
    unit_group_id,
    null as dimension_result_category_id,
    unit_group_type as metric_grouper,
    score_to_rank as numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    rank_the_score
where
    type_score_rank <= 7
	and unit_group_type = 'Ambulatory'

union all

select
    'RNdimTop8acute' as metric_abbreviation,
    metric_year,
    rn_dimension_id,
    nurse_cohort_id,
    null as job_group_id,
    null as engagement_question_id,
    unit_group_id,
    null as dimension_result_category_id,
    unit_group_type as metric_grouper,
    score_to_rank as numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    rank_the_score
where
    type_score_rank <= 8
	and unit_group_type = 'Acute'
union all

select
    'YoyTrendKPI' as metric_abbreviation,
    yoy_result.metric_year,
    yoy_result.rn_dimension_id,
    yoy_result.nurse_cohort_id,
    yoy_result.job_group_id,
    yoy_result.engagement_question_id,
    yoy_result.unit_group_id,
    yoy_result.dimension_result_category_id,
    yoy_result.metric_grouper,
    sum( case when
        rslt_catg.yoy_trend_category = 'improved'
        or dimension_result_category_abbreviation in (
            'New to Exceeds',
			'Exceeds to Exceeds')
        then 1 else 0 end) as kpi_numerator,
    1 as kpi_denominator,
    --kpi_numerator / kpi_denominator
	null::numeric  as row_metric_calculation
from
    {{ ref('stg_nursing_engage_w5_yoy') }} as yoy_result
	inner join {{ ref('lookup_nursing_survey_dimension_result_category') }} as rslt_catg
            on yoy_result.dimension_result_category_id = rslt_catg.dimension_result_category_id
where
    engagement_question_id is null /* trending for dimenion only */
group by
    yoy_result.metric_year,
    yoy_result.rn_dimension_id,
    yoy_result.nurse_cohort_id,
    yoy_result.job_group_id,
    yoy_result.engagement_question_id,
    yoy_result.unit_group_id,
    yoy_result.dimension_result_category_id,
    yoy_result.metric_grouper
