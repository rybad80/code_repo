{{ config(meta = {
    'critical': true
}) }}

/* stg_nccs_safety_obs_role_furlough
from the non-productive direct hours, capture the subset which were safety
observation workers on furlough as recorded from Kronos
*/
select
    'SafetyObsWfurlough' as metric_abbreviation,
    time_off.worker_id,
    time_off.pp_end_dt_key,
    time_off.metric_date,
    time_off.metric_dt_key,
    time_off.company_id,
    time_off.cost_center_id,
    time_off.cost_center_site_id,
    time_off.job_code,
    case time_off.job_group_id
        when 'psychTech' then 'PTECH'
        when 'Sitter' then time_off.job_group_id
        else 'otherSafetyObsJob'
    end as job_role_grouper,
    time_off.job_group_id,
    safety_obs_job.nursing_job_grouper,
    time_off.timereport_paycode,
    time_off.nursing_business_report_select_ind,
    time_off.cost_center_parent,
    time_off.cost_center_type,
    time_off.cost_center_group,
    time_off.provider_or_other_job_group_id,
    time_off.rn_alt_or_other_job_group_id,
    time_off.job_organization_id,
    sum(time_off.non_direct_daily_hours) as furlough_timeoff_hours,
    sum(time_off.non_direct_daily_full_time_percentage) as furlough_timeoff_fte,
    1 as one_on_one_safety_obs_job_ind,
    nursing_pay_period.prior_pay_period_ind,
    nursing_pay_period.future_pay_period_ind
from
    {{ ref('stg_timejob_timeorg') }} as timeorg
    inner join {{ ref('timereport_daily_non_direct') }} as time_off
        on time_off.job_organization_id = timeorg.job_organization_id
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on time_off.pp_end_dt_key = nursing_pay_period.pp_end_dt_key
    inner join {{ ref('stg_nursing_job_code_group') }} as safety_obs_job
        on time_off.job_code = safety_obs_job.job_code
        and safety_obs_job.provider_job_group_id in (
            'Sitter',
            'psychTech',
            'SafetyObserver')
where
    time_off.timereport_paycode in (
            'PPLS FURLOUGH',
            'EXCUSE ABS-FURLOUGH',
            'EXCUSE ABS')
group by
    time_off.worker_id,
    time_off.pp_end_dt_key,
    time_off.metric_date,
    time_off.metric_dt_key,
    time_off.company_id,
    time_off.cost_center_id,
    time_off.cost_center_site_id,
    time_off.job_code,
    job_role_grouper,
    time_off.job_group_id,
    safety_obs_job.nursing_job_grouper,
    time_off.timereport_paycode,
    time_off.nursing_business_report_select_ind,
    time_off.cost_center_parent,
    time_off.cost_center_type,
    time_off.cost_center_group,
    time_off.provider_or_other_job_group_id,
    time_off.rn_alt_or_other_job_group_id,
    time_off.job_organization_id,
    one_on_one_safety_obs_job_ind,
    nursing_pay_period.prior_pay_period_ind,
    nursing_pay_period.future_pay_period_ind
