/* stg_nursing_staff_p7f_termination_total
part 7 step f by cost center create metrics about Workday terminations from RN/UPA/LPN jobs this past year
regular, new hires, last 30/365 days, and the tenured (NotnewHireTerm) versus in last year hires
for the waterfall
*/

with
union_set as (
    select
        job_group_id,
        case hire_in_last_year_ind
            when 1 then 'LastYrNewHireTermCnt'
            else 'LastYrNotnewHireTermCnt' end as metric_abbreviation,
        cost_center_id,
        sum(month_term_cnt) as numerator /* last 365 days by hire label termination counts */
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        term_in_last_year_ind = 1
    group by
        case hire_in_last_year_ind
            when 1 then 'LastYrNewHireTermCnt'
            else 'LastYrNotnewHireTermCnt' end,
        job_group_id,
        cost_center_id

    union all

    select
        job_group_id,
        'LastYrTermCnt' as metric_abbreviation,
        cost_center_id,
        sum(month_term_cnt) as numerator /* last 365 days termination counts */
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        term_in_last_year_ind = 1
    group by
        job_group_id,
        cost_center_id

    union all

    select
        job_group_id,
        'LastYrTermFTE' as metric_abbreviation,
        cost_center_id,
        sum(month_term_fte) as numerator /* last 365 days termination fte */
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        term_in_last_year_ind = 1
    group by
        job_group_id,
        cost_center_id

    union all

    select
        job_group_id,
        'Last30NewHireTermCnt' as metric_abbreviation,
        cost_center_id,
        sum(month_term_cnt) as numerator /* last thirty days NewHire termination counts */
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        term_last_thirty_days_ind  = 1
        and hire_in_last_year_ind = 1
    group by
        job_group_id,
        cost_center_id

    union all

    select
        job_group_id,
        'Last30NewHireVolTermCnt' as metric_abbreviation,
        cost_center_id,
        sum(month_term_cnt) as numerator /* last 30 days NewHire Voluntary ttermination counts */
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        term_last_thirty_days_ind  = 1
        and hire_in_last_year_ind = 1
        and termination_involuntary_ind = 0
    group by
        job_group_id,
        cost_center_id

    union all

    select
        job_group_id,
        'Last30TermCnt' as metric_abbreviation,
        cost_center_id,
        sum(month_term_cnt) as numerator /* last thirty days termination counts */
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        term_last_thirty_days_ind  = 1
    group by
        job_group_id,
        cost_center_id

    union all

    select
        job_group_id,
        'Last30VolTermCnt' as metric_abbreviation,
        cost_center_id,
        sum(month_term_cnt) as numerator /* last thirty days Voluntary termination counts */
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        term_last_thirty_days_ind  = 1
        and termination_involuntary_ind = 0
    group by
        job_group_id,
        cost_center_id

    union all

    select
        job_group_id,
        'Last30TermFTE' as metric_abbreviation,
        cost_center_id,
        sum(month_term_fte) as numerator /* last thirty days termination fte */
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        term_last_thirty_days_ind  = 1
    group by
        job_group_id,
        cost_center_id
)

select
    union_set.metric_abbreviation,
    dim_date.date_key as metric_dt_key,
    union_set.cost_center_id,
    job_group_id,
    null as metric_grouper,
    union_set.numerator,
    null::numeric as denominator,
    union_set.numerator as row_metric_calculation
from
    union_set
    inner join dim_date
        on dim_date.full_date = current_date - 1
