{{ config(meta = {
    'critical': true
}) }}

/* timereport_daily_non_direct
organizes the time that is NOT productive direct into job groups and sets additional key
indicators and job and cost center grouping attributes as well as calculates FTE
Note:  this non-Direct time is
-- Productive Indirect (working but not on main job tasks, includes Orientation, Training, Conference
     or Regular Indirect which Nursing calls Project Time)
-- Non-productive (which is time off, not working -- and people are generally paid for the time such as PPL)
Note: other pay specific addendums related to time such as premium pay or bonus exempt are captured
in timereport_daily_other
Direct time records are in timereport__daily_productive_direct
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
    daily_hours.time_subset as non_direct_subset,
    daily_hours.non_productive_ind,
    nursing_report_cost_centers.nursing_business_report_select_ind,
    nursing_report_cost_centers.cost_center_parent,
    nursing_report_cost_centers.cost_center_type,
    nursing_report_cost_centers.cost_center_group,
    daily_hours.worker_daily_total / 80 as non_direct_daily_full_time_percentage,
    daily_hours.worker_daily_total as non_direct_daily_hours,
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
    non_productive_ind = 1 /* time off hours */
    or productive_indirect_ind = 1 /* ( NOT 'regular', including callback or 'overtime' pay codes */
