/* stg_nursing_engage_w5_yoy
for matched nurse-cohort/RN excellence dimension/Question/Unit group
combinations year over year, combine each "result against benchmark"
with the result of the prior year to get groupings such as:
  Met / Just Under --> Outperform
  new grouper --> Met / Just under
  Outperform --> Below
*/
with
match_rollup_metric as (
    select
        category_metric_abbreviation,
        yoy_metric_abbreviation
    from
        {{ ref('nursing_metric_mapping_engage') }}
    where
        benchmark_metric_abbreviation is not null
),

catg_result as (
    select
        match_rollup_metric.category_metric_abbreviation,
		match_rollup_metric.yoy_metric_abbreviation,
        metric_year,
        rn_dimension_id,
        nurse_cohort_id,
        engagement_question_id,
        unit_group_id,
        dimension_result_category_id,
        metric_grouper,
        row_metric_calculation as category_row_metric_calculation,
        coalesce(engagement_question_id, '0') as match_engagement_question_id,
        coalesce(unit_group_id, '0') as match_unit_group_id

    from
         {{ ref('stg_nursing_engage_w4_category') }} as stg_nursing_engage_w4_category
		inner join match_rollup_metric
		on  stg_nursing_engage_w4_category.metric_abbreviation
		= match_rollup_metric.category_metric_abbreviation
),

compare_prior_year as (
    select
        catg_result.yoy_metric_abbreviation as metric_abbreviation,
        catg_result.metric_year,
        catg_result.rn_dimension_id,
        catg_result.nurse_cohort_id,
        catg_result.engagement_question_id,
        catg_result.unit_group_id,
        yoy_rslt_catg.dimension_result_category_id,
        catg_result.metric_grouper,
        case
            when catg_result_prior_yr.category_metric_abbreviation is null
            then 1 else 0
            end as new_unit_group_ind,
        catg_result.dimension_result_category_id as new_grouper_rslt_catg_id,
        round(catg_result.category_row_metric_calculation, 2) as numerator,
        round(catg_result_prior_yr.category_row_metric_calculation, 2) as denominator,
        catg_result.category_row_metric_calculation
        - catg_result_prior_yr.category_row_metric_calculation as row_metric_calculation
    from
        catg_result
        left join catg_result as catg_result_prior_yr
            on catg_result.metric_year = catg_result_prior_yr.metric_year + 1
            and catg_result.category_metric_abbreviation
            = catg_result_prior_yr.category_metric_abbreviation
            and catg_result.rn_dimension_id = catg_result_prior_yr.rn_dimension_id
            and catg_result.nurse_cohort_id = catg_result_prior_yr.nurse_cohort_id
            and catg_result.match_engagement_question_id
            = catg_result_prior_yr.match_engagement_question_id
            and catg_result.match_unit_group_id = catg_result_prior_yr.match_unit_group_id
        left join {{ ref('lookup_nursing_survey_dimension_result_category') }} as yoy_rslt_catg
            on catg_result_prior_yr.dimension_result_category_id
            = yoy_rslt_catg.prior_dimension_result_category_id
            and catg_result.dimension_result_category_id
            = yoy_rslt_catg.next_dimension_result_category_id
    where
        catg_result.metric_year > 2019
)

select
    compare_prior_year.metric_abbreviation,
    compare_prior_year.metric_year,
    compare_prior_year.rn_dimension_id,
    compare_prior_year.nurse_cohort_id,
    null as job_group_id,
    compare_prior_year.engagement_question_id,
    compare_prior_year.unit_group_id,
    coalesce(compare_prior_year.dimension_result_category_id,
        new_grp_rslt_catg.dimension_result_category_id)
        as dimension_result_category_id,
    compare_prior_year.metric_grouper,
    compare_prior_year.numerator,
    compare_prior_year.denominator,
    compare_prior_year.row_metric_calculation
from
    compare_prior_year
    left join {{ ref('lookup_nursing_survey_dimension_result_category') }} as new_grp_rslt_catg
        on compare_prior_year.new_grouper_rslt_catg_id
        = new_grp_rslt_catg.next_dimension_result_category_id
		and new_grp_rslt_catg.new_grouping_ind = 1
