/* stg_nursing_staff_p7c_rn_monthly
part 7 step c by cost center generate monthly total metrics (counts and FTE)
for Workday hires and terminations for year at a glance run charts */

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
		job_group_id,
		'RN' as met_abbrev_prefix
    from
        {{ ref('stg_nursing_staff_p7a_hire') }}
    where
        rn_job_ind = 1
),

termination_data as (
    select
        worker_id,
        cost_center_id,
        month_pp_end_dt_key,
        1 as month_term_cnt,
        month_term_fte,
        term_last_thirty_days_ind,
        term_in_last_year_ind,
        hire_in_last_year_ind,
        termination_involuntary_ind,
        term_category,
		job_group_id,
		'RN' as met_abbrev_prefix
    from
        {{ ref('stg_nursing_staff_p7b_termination') }}
    where
        rn_job_ind = 1
),

monthly_union_set as (
    select
        met_abbrev_prefix || 'MonHireCnt' as metric_abbreviation,
        cost_center_id,
        month_pp_end_dt_key as metric_dt_key,
        sum(month_hire_cnt) as numerator /* monthly hire counts */
    from
        hire_data
    where
        month_hire_cnt = 1
    group by
        cost_center_id,
        month_pp_end_dt_key,
        met_abbrev_prefix

    union all

    select
        met_abbrev_prefix || 'MonHireFTE' as metric_abbreviation,
        cost_center_id,
        month_pp_end_dt_key as metric_dt_key,
        sum(month_hire_fte) as numerator /* monthly hire fte */
    from
        hire_data
    where
        month_hire_cnt = 1
    group by
        cost_center_id,
        month_pp_end_dt_key,
        met_abbrev_prefix

    union all

    select
        'RNMonTermCnt' as metric_abbreviation,
        cost_center_id,
        month_pp_end_dt_key as metric_dt_key,
        sum(month_term_cnt) as numerator /* termination coutns by month */
    from
        termination_data
    where
        month_term_cnt = 1
    group by
        cost_center_id,
        month_pp_end_dt_key

    union all

    select
        'RNMonTermFTE' as metric_abbreviation,
        cost_center_id,
        month_pp_end_dt_key as metric_dt_key,
        sum(month_term_fte) as numerator /* termination coutns by month */
    from
        termination_data
    where
        month_term_cnt = 1
    group by
        cost_center_id,
        month_pp_end_dt_key
)

select
    union_set.metric_abbreviation,
    union_set.cost_center_id,
	union_set.metric_dt_key,
    'CHOPRN' as job_group_id,
	null as metric_grouper,
    union_set.numerator,
    null::numeric as denominator,
    union_set.numerator as row_metric_calculation
from
    monthly_union_set as union_set
