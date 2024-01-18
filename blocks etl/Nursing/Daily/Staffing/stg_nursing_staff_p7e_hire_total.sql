/* stg_nursing_staff_p7e_hire_total
part 7 step e by cost center create metrics about Workday hires in RN/LPN/UAP jobs this past year
new hires, rehires, last 365 days
and the starting number for the employment delta waterfalls */

with
hire_data as (
    select
        worker_id,
        cost_center_id,
        month_pp_end_dt_key,
        month_hire_cnt,
        month_hire_fte,
        hire_in_last_year_ind,
        active_ind,
        term_in_last_year_ind,
        employment_rehired_ind,
        job_group_id
    from
        {{ ref('stg_nursing_staff_p7a_hire') }}
),

union_set as (
    select
        'LastYrActvRehireCnt' as metric_abbreviation,
        cost_center_id,
        job_group_id,
        sum(month_hire_cnt) as numerator /* last 365 days active rehire counts */
    from
        hire_data
    where
        hire_in_last_year_ind = 1
        and employment_rehired_ind = 1
        and active_ind = 1
    group by
        cost_center_id,
        job_group_id

    union all

    select
        'LastYrRehireCnt' as metric_abbreviation,
        cost_center_id,
        job_group_id,
        sum(month_hire_cnt) as numerator /* last 365 days rehire counts */
    from
        hire_data
    where
        hire_in_last_year_ind = 1
        and employment_rehired_ind = 1
    group by
        cost_center_id,
        job_group_id

    union all /* starting count of the waterfalls for RN/LPN/UAP deltas */

    select
        'YrAgoCnt' as metric_abbreviation,
        cost_center_id,
        case rn_job_ind when 1
            then 'RN'
            else nursing_category_abbreviation end as job_group_id,
        count(*) as numerator /* current RN count */
    from
        {{ ref('nursing_worker') }}
    where /*  active a year ago */
        ((active_ind = 1 and coalesce(hire_in_last_year_ind, 0) = 0)
        or (active_ind = 0 and term_in_last_year_ind = 1
            and hire_timeframe_label = 'over a year at CHOP' )
        )
        and ( /* worker cohort */
        rn_job_ind = 1
        or nursing_category_abbreviation in ('UAP', 'LPN')
        )
    group by
        cost_center_id,
        case rn_job_ind when 1
            then 'RN'
            else nursing_category_abbreviation end

    union all /* will become denominator for the RNLastYrNewHirePct*/

    select
        'RNcurrentCnt' as metric_abbreviation,
        cost_center_id,
        'RN' as job_group_id,
        sum(active_ind) as numerator /* current RN count */
    from
        {{ ref('nursing_worker') }}
    where
        active_ind = 1
        and rn_job_ind = 1
    group by
        cost_center_id

    union all

    select
        'LastYrNewHireCnt' as metric_abbreviation, /* getting numerator for the RNLastYrNewHirePct */
        cost_center_id,
        job_group_id,
        sum(month_hire_cnt) as numerator /* last 365 days hire still here counts */
    from
        hire_data
    where
        hire_in_last_year_ind = 1
        and coalesce(employment_rehired_ind, 0) = 0 /* not a rehire*/
        and coalesce(term_in_last_year_ind, 0) = 0 /* still here at CHOP */
    group by
        cost_center_id,
        job_group_id

    union all

    select
        'LastYrHireCnt' as metric_abbreviation,
        cost_center_id,
        job_group_id,
        sum(month_hire_cnt) as numerator /* last 365 days hire counts */
    from
        hire_data
    where
        hire_in_last_year_ind = 1
    group by
        cost_center_id,
        job_group_id

    union all

    select
        'LastYrHireFTE' as metric_abbreviation,
        cost_center_id,
        job_group_id,
        sum(month_hire_fte) as numerator /* last 365 days hire fte */
    from
        hire_data
    where
        hire_in_last_year_ind = 1
    group by
        cost_center_id,
        job_group_id
)

select
    union_set.metric_abbreviation,
    dim_date.date_key as metric_dt_key,
    union_set.cost_center_id,
    union_set.job_group_id,
    null as metric_grouper,
    union_set.numerator,
    null::numeric as denominator,
    union_set.numerator as row_metric_calculation
from
    union_set
    inner join dim_date
        on dim_date.full_date = current_date - 1
