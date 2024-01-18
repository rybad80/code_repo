{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_finance_staffing_monthly
Still need to process the budgets from a lookup as of FY 2023 and 2024 (per Tyler Manning).
Roll the month data to individual rows for each job code's aggregate of productive and
non-productive time (Time Off, finance labels as PPL or Other) for fixed or variable job roles
and also roll up to the job_group that applies for each job code assigned to people
*/
with variable_job as (
    select
        lookup_job_group_set.usage,
        lookup_job_group_set.set_desc,
        lookup_job_group_set.job_group_id,
        lookup_job_group_set.effective_thru_fiscal_year
    from
        {{ ref('lookup_job_group_set') }} as lookup_job_group_set
    where
        lookup_job_group_set.usage = 'flex component'
        and lookup_job_group_set.set_desc = 'variable'
),

cc_job_month_budget as (
    select
        staff_budget.budget_paycode_grouper,
        coalesce(staff_budget.budget_paycode_grouper, 'NULL') as match_paycode_grouper,
        staff_budget.month_staff_budget_fte,
        staff_budget.company_id,
        staff_budget.cost_center_id,
        staff_budget.cost_center_site_id,
        staff_budget.job_code,
        staff_budget.fiscal_year,
        staff_budget.fiscal_month_num,
        case
            when staff_budget.fiscal_year <= variable_job.effective_thru_fiscal_year
            then 1
            when staff_budget.fiscal_year > variable_job.effective_thru_fiscal_year
            then 0
            else 0
        end as variable_ind,
        case
            when get_job_group.fixed_rn_override_ind = 1
            then get_job_group.rn_alt_job_group_id
            else get_job_group.use_job_group_id
        end as job_group_id,
        cc.nursing_flex_ind as cc_nursing_flex_ind,
        cc.flex_variable_ind as cc_flex_variable_ind
    from
        {{ ref('lookup_nursing_staffing_month_budget') }} as staff_budget
    inner join {{ ref('nursing_cost_center_attributes') }} as cc
        on staff_budget.cost_center_id = cc.cost_center_id
    inner join {{ ref('stg_nursing_job_code_group_statistic') }} as get_job_group
        on staff_budget.job_code = get_job_group.job_code
    left join variable_job
        on variable_job.job_group_id = get_job_group.use_job_group_id
)

select /* rows aggregated by job code's job group */
     case when cc_job_month_budget.variable_ind = 0
        /* exception: the Ambulatory RNs are staff nurses that are a fixed role, not variable */
            or  (cc_job_month_budget.job_group_id = 'AmbulatoryRN' and cc_job_month_budget.variable_ind = 1)
            or  (cc_job_month_budget.cc_flex_variable_ind = 0 /* if the cost center is not variable, all */
                and cc_job_month_budget.cc_nursing_flex_ind = 1 /* rolese are fixed for flex targets */
                and cc_job_month_budget.variable_ind = 1) /* even if the role is variable for acute */
        then case
            when cc_job_month_budget.budget_paycode_grouper is null
            then 'nullJgFixed'
            when cc_job_month_budget.budget_paycode_grouper = 'Paid Personal Leave'
            then 'JgBdgtPPLnonPrdctvFixed'
            when cc_job_month_budget.budget_paycode_grouper = 'Other FTE'
            then 'JgBdgtOthnonPrdctvFixed'
            when cc_job_month_budget.budget_paycode_grouper = 'Other FTE - Parental Leave'
            then 'JgBdgtParLVnonPrdctvFixed'
            else 'JgBdgtPrdctvFixed'
            end
        else case
            /* a variable role in the various categories for job group */
            when cc_job_month_budget.budget_paycode_grouper is null
            then 'nullJgVar'
            when cc_job_month_budget.budget_paycode_grouper = 'Paid Personal Leave'
            then 'JgBdgtPPLnonPrdctvVar'
            when cc_job_month_budget.budget_paycode_grouper = 'Other FTE'
            then 'JgBdgtOthnonPrdctvVar'
            when cc_job_month_budget.budget_paycode_grouper = 'Other FTE - Parental Leave'
            then 'JgBdgtParLVnonPrdctvVar'
            else 'JgBdgtPrdctvVar'
            end
        end as metric_abbreviation,

        cc_job_month_budget.job_group_id,
        cc_job_month_budget.cost_center_id,
        cc_job_month_budget.cost_center_site_id,
        cc_job_month_budget.fiscal_year,
        cc_job_month_budget.fiscal_month_num,
        cc_job_month_budget.budget_paycode_grouper as metric_grouper,
        round(sum(cc_job_month_budget.month_staff_budget_fte), 2) as numerator,
        null as job_code
from
    cc_job_month_budget
group by
    cc_job_month_budget.job_group_id,
    cc_job_month_budget.cost_center_id,
    cc_job_month_budget.cost_center_site_id,
    cc_job_month_budget.fiscal_year,
    cc_job_month_budget.fiscal_month_num,
    cc_job_month_budget.budget_paycode_grouper,
    cc_job_month_budget.variable_ind,
	cc_job_month_budget.cc_flex_variable_ind,
    cc_job_month_budget.cc_nursing_flex_ind

union all

select /* rows aggregated by job cdoe */
    case when cc_job_month_budget.variable_ind = 0
        /* exception: the Ambulatory RNs are staff nurses that are a fixed role, not variable */
            or  (cc_job_month_budget.job_group_id = 'AmbulatoryRN' and cc_job_month_budget.variable_ind = 1)
            or  (cc_job_month_budget.cc_flex_variable_ind = 0 /* if the cost center is not variable, all */
                and cc_job_month_budget.cc_nursing_flex_ind = 1 /* rolese are fixed for flex targets */
                and cc_job_month_budget.variable_ind = 1) /* even if the role is variable for acute */
        then case
        when cc_job_month_budget.budget_paycode_grouper is null
        then 'nullJcFixed'
        when cc_job_month_budget.budget_paycode_grouper = 'Paid Personal Leave'
        then 'JobCodeBdgtPPLnonPrdctvFixed'
        when cc_job_month_budget.budget_paycode_grouper = 'Other FTE'
        then 'JobCodeBdgtOthnonPrdctvFixed'
        when cc_job_month_budget.budget_paycode_grouper = 'Other FTE - Parental Leave'
        then 'JobCodeBdgtParLVnonPrdctvFixed'
        else 'JobCodeBdgtPrdctvFixed'
        end
    else case
        /* a variable role in the various categories for job code */
        when cc_job_month_budget.budget_paycode_grouper is null
        then 'nullJcVar'
        when cc_job_month_budget.budget_paycode_grouper = 'Paid Personal Leave'
        then 'JobCodeBdgtPPLnonPrdctvVar'
        when cc_job_month_budget.budget_paycode_grouper = 'Other FTE'
        then 'JobCodeBdgtOthnonPrdctvVar'
        when cc_job_month_budget.budget_paycode_grouper = 'Other FTE - Parental Leave'
        then 'JobCodeBdgtParLVnonPrdctvVar'
        else 'JobCodeBdgtPrdctvVar'
        end
    end as metric_abbreviation,

    cc_job_month_budget.job_group_id,
    cc_job_month_budget.cost_center_id,
    cc_job_month_budget.cost_center_site_id,
    cc_job_month_budget.fiscal_year,
    cc_job_month_budget.fiscal_month_num,
    cc_job_month_budget.budget_paycode_grouper as metric_grouper,
    round(sum(cc_job_month_budget.month_staff_budget_fte), 2) as numerator,
    cc_job_month_budget.job_code
from cc_job_month_budget
group by
    cc_job_month_budget.job_group_id,
    cc_job_month_budget.cost_center_id,
    cc_job_month_budget.cost_center_site_id,
    cc_job_month_budget.fiscal_year,
    cc_job_month_budget.fiscal_month_num,
    cc_job_month_budget.budget_paycode_grouper,
    cc_job_month_budget.job_code,
    cc_job_month_budget.variable_ind,
	cc_job_month_budget.cc_flex_variable_ind,
    cc_job_month_budget.cc_nursing_flex_ind
