{{ config(meta = {
    'critical': true
}) }}

/* timereport_daily_other
organizes the time that is for additional program incentive for staff to take shifts and
aggregates into job groups and sets additional key indicators and job and cost center
grouping attributes
Note:  this reflects records in Kronos that go alongside the Direct REGULAR & OVERTIME (and
are not really worked hours but amounts that turn into some sort of pay incentive or other
record for staffing time of employees)
*/

with job_attributes as (
    select
        job_code,
        job_title_display,
        rn_job_ind,
        nursing_job_grouper as job_group_id,
        provider_or_other_job_group_id,
        rn_alt_or_other_job_group_id
    from
        {{ ref('stg_nursing_job_code_group') }}
),

exclude_paycode_grouper as ( /* time not REGULAR and not OVERTIME
    or the other non-Direct codes */
    select
        attribute_type,
        wf_kronos_code,
        attribute_value as time_subset,
        productive_type
    from
        {{ ref('lookup_paycode_attribute') }}
    where
        productive_type in (
            'direct',
            'indirect',
            'non'
        )
)

select
    daily_rec.worker_id,
    daily_rec.pp_end_dt_key,
    daily_rec.metric_date,
    daily_rec.metric_dt_key,
    daily_rec.company_id,
    daily_rec.cost_center_id,
    daily_rec.cost_center_site_id,
    daily_rec.timereport_org_path,
    daily_rec.job_code,
    daily_rec.hppd_job_group_id,
    job_attributes.job_group_id,
    job_attributes.rn_job_ind,
    daily_rec.timereport_paycode,
    paycode_induce.paycode_hours_category as inducement_category,
    case
        when paycode_induce.paycode_hours_category is not null
        then 1 else 0
    end as inducement_ind,
    case
        when paycode_induce.paycode_hours_category is not null then 'inducement'
        when daily_rec.timereport_paycode like 'BONUS EXEMPT%' then 'bonusexempt'
        when daily_rec.timereport_paycode like 'SHIFT RATE%' then 'shiftrate'
        when daily_rec.timereport_paycode like 'FLEX%' then 'flex'
        when daily_rec.timereport_paycode like 'ON CALL%' then 'oncall'
        when daily_rec.timereport_paycode like 'LV%' then 'leave'
        when daily_rec.timereport_paycode like 'CALLOUT%' then 'callout'
        when daily_rec.timereport_paycode like 'MOONLIGHT%' then 'moonlight'
        when daily_rec.timereport_paycode like 'EXTRA SHIFT%' then 'extrashift'
        else 'tbd'
    end as other_type,
    nursing_report_cost_centers.nursing_business_report_select_ind,
    nursing_report_cost_centers.cost_center_parent,
    nursing_report_cost_centers.cost_center_type,
    nursing_report_cost_centers.cost_center_group,
    daily_rec.worker_daily_total,
    daily_rec.money_daily_total,
    job_attributes.provider_or_other_job_group_id,
    job_attributes.rn_alt_or_other_job_group_id,
    daily_rec.wfctotal_id,
    daily_rec.timereport_paycode_id,
    daily_rec.productivity_type_id,
    daily_rec.timesheet_item_id,
    daily_rec.job_organization_id
from
    {{ ref('timereport_daily_all') }} as daily_rec
    inner join job_attributes
        on daily_rec.job_code = job_attributes.job_code
    inner join {{ ref('nursing_cost_center_attributes') }} as nursing_report_cost_centers
        on daily_rec.cost_center_id = nursing_report_cost_centers.cost_center_id
    left join {{ ref('lookup_paycode_group_category') }} as paycode_induce
        on lower(daily_rec.timereport_paycode) = lower(paycode_induce.kronos_paycode_group)
        and lower(paycode_induce.paycode_hours_category) = 'inducement'
    left join exclude_paycode_grouper
        on lower(daily_rec.timereport_paycode) = lower(exclude_paycode_grouper.wf_kronos_code)
where
    /* exlcude the records already captured by the productive_direct and non_direct tables */
    exclude_paycode_grouper.wf_kronos_code is null
