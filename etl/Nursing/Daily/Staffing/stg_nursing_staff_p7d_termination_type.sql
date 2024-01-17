/* stg_nursing_staff_p7d_termination_type
part 7 step d by cost center create subsets of Workday terminations from RN/UAP/LPN jobs this past year
aggregating into new hires & not with hire date in last 365 days, voluntary & involuntary */

with
voluntary_or_not as (
    select
        job_group_id,
        'LastYrNotnewHireTermTypeCnt' as metric_abbreviation, /* not new hire */
        cost_center_id,
        term_category as metric_grouper,
        sum(month_term_cnt) as numerator /* last 365 days termination counts */
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        term_in_last_year_ind = 1
        and hire_in_last_year_ind = 0
    group by
        job_group_id,
        cost_center_id,
        term_category

    union all
    select
        job_group_id,
        'LastYrNewHireTermTypeCnt' as metric_abbreviation, /* new hires */
        cost_center_id,
        term_category as metric_grouper,
        sum(month_term_cnt) as numerator /* last 365 days termination counts */
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        term_in_last_year_ind = 1
        and hire_in_last_year_ind = 1
    group by
        job_group_id,
        cost_center_id,
        term_category
)

select
    metric_abbreviation,
    dim_date.date_key as metric_dt_key,
    cost_center_id,
    job_group_id,
    metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    voluntary_or_not
    inner join dim_date
        on dim_date.full_date = current_date - 1
