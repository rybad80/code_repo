/* stg_nursing_staff_w2_budget
gather the budget for each job_group but only down to
a maximum level 4 granularity and thus rolling any level 5
up to its level 4
And also do capture the budgets for job codes that have fallen
into their default XXXjob_category job group
And give a group to the 'FVJC'
budget job codes for 'Fixed Vancancy job Code' that give
a (budgeted) buffer in general of likely outstanding vacancy at any
point in the year for the cost center overall
*/

select
    'currFTEBdgtLvl4' as metric_abbreviation,
    budget.metric_dt_key,
    null as worker_id,
    budget.cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    coalesce(
        --lvls.level_4_id,  /* take the lvl4 if one applies */
        --lvls.job_group_id, /* else the higher level job group */
        --njcgs.nursing_job_grouper, /* but if not in the levels table get the XXX one */
        lvls.nursing_job_rollup,
        case
            when njcgs.fixed_rn_override_ind = 1
            then njcgs.rn_alt_job_group_id
            else njcgs.use_job_group_id
        end,
        fvjc.job_group, /* lastly handling the Fixed Vacancy 'special case' job_code */
        'unk Job Grp ID') as job_group_id,
    null as metric_grouper,
    sum(budget.numerator) as numerator,
    null::numeric as denominator,
    sum(budget.numerator) as row_metric_calculation
from {{ ref('stg_nursing_budget_period_workforce') }} as budget
inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
    on budget.metric_dt_key = nursing_pay_period.pp_end_dt_key
    and nursing_pay_period.current_working_pay_period_ind = 1
left join {{ ref('stg_nursing_job_code_group_statistic') }} as njcgs
    on budget.job_code = njcgs.job_code
left join {{ ref('job_group_levels_nursing') }} as lvls
    on case
        when njcgs.fixed_rn_override_ind = 1
        then njcgs.rn_alt_job_group_id
        else njcgs.use_job_group_id
    end = lvls.job_group_id
left join {{ ref('lookup_job_group_direct_job_code') }} as fvjc
    on budget.job_code = fvjc.job_code
where
    budget.metric_abbreviation = 'staffCCbdgt'
group by
    budget.metric_dt_key,
    budget.cost_center_id,
    coalesce(
        --lvls.level_4_id,
        --lvls.job_group_id,
        lvls.nursing_job_rollup,
        case
            when njcgs.fixed_rn_override_ind = 1
            then njcgs.rn_alt_job_group_id
            else njcgs.use_job_group_id
        end,
        fvjc.job_group,
        'unk Job Grp ID')
