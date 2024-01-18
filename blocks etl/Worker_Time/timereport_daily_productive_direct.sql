{{ config(meta = {
    'critical': true
}) }}

/* timereport_daily_productive_direct
organizes the productive direct time into job groups and sets additional key
indicators and job and cost center grouping attributes as well as calculates FTE
Note:  Productive Direct time is where workers spend most of their time doing
their job, and that mostly as REGULAR:  other non-direct pay codes group to one of
-- Productive Indirect
-- Non-productive (which is time off, not working)
and for which records are in the timereport_daily_non_direct
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
)

select
    daily_hours.worker_id,
    daily_hours.pp_end_dt_key,
    daily_hours.metric_date,
    daily_hours.metric_dt_key,
    daily_hours.company_id,
    daily_hours.cost_center_id,
    daily_hours.cost_center_site_id,
    daily_hours.job_code,
    daily_hours.hppd_job_group_id,
    job_attributes.job_group_id,
    job_attributes.rn_job_ind,
    daily_hours.timereport_paycode,
    case daily_hours.time_subset
        when 'overtime' then 1 else 0
    end as overtime_ind,
    nursing_report_cost_centers.nursing_business_report_select_ind,
    nursing_report_cost_centers.cost_center_parent,
    nursing_report_cost_centers.cost_center_type,
    nursing_report_cost_centers.cost_center_group,
    daily_hours.worker_daily_total / 80 as productive_direct_daily_full_time_percentage,
    daily_hours.worker_daily_total as productive_direct_daily_hours,
    job_attributes.provider_or_other_job_group_id,
    job_attributes.rn_alt_or_other_job_group_id,
    daily_hours.wfctotal_id,
    daily_hours.timereport_paycode_id,
    daily_hours.productivity_type_id,
    daily_hours.timesheet_item_id,
    daily_hours.labor_accounting_id,
    daily_hours.job_organization_id,
    daily_hours.nccs_platform_window_ind
from
    {{ ref('timereport_daily_all') }} as daily_hours
    inner join job_attributes
        on daily_hours.job_code = job_attributes.job_code
    inner join {{ ref('nursing_cost_center_attributes') }} as nursing_report_cost_centers
        on daily_hours.cost_center_id = nursing_report_cost_centers.cost_center_id
where
    productive_direct_ind = 1 /* ('regular', 'overtime') pay codes */
    and callback_ind = 0 /* until further notice, do not incldue callback time for Nursing downstream */
