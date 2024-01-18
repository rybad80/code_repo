{{ config(meta = {
    'critical': true
}) }}

/* stg_nccs_safety_obs_role_indicators
from the productive direct hours, capture the subset which were safety observations
as recorded from Kronos including the indicators
--general safety observation per the org path,
--Meals out of Room (MOOR)
--a job code whose job is 100% of direct time as a safety observer
*/

select
    'SafetyObsPPinds' as metric_abbreviation,
    direct.worker_id,
    direct.pp_end_dt_key,
    direct.metric_date,
    direct.metric_dt_key,
    direct.company_id,
    direct.cost_center_id,
    direct.cost_center_site_id,
    direct.job_code,
    case direct.job_group_id
        when 'SNA' then direct.job_group_id
        when 'psychTech' then 'PTECH'
        when 'ACT' then direct.job_group_id
        when 'nurseTech' then 'NTECH'
        when 'nurseCoopII' then 'CO-OP'
        when 'Sitter' then direct.job_group_id
        else 'OTHER'
    end as job_role_grouper,
    direct.hppd_job_group_id,
    direct.job_group_id,
    safety_obs_job.nursing_job_grouper,
    direct.rn_job_ind,
    direct.timereport_paycode,
    direct.overtime_ind,
    direct.nursing_business_report_select_ind,
    direct.cost_center_parent,
    direct.cost_center_type,
    direct.cost_center_group,
    direct.provider_or_other_job_group_id,
    direct.rn_alt_or_other_job_group_id,
    direct.job_organization_id,
    sum(direct.productive_direct_daily_hours) as safety_obs_hours,
    sum(direct.productive_direct_daily_full_time_percentage) as safety_obs_fte,
    timeorg.orgpath_safety_obs_ind,
    timeorg.orgpath_meal_out_of_room_ind,
    timeorg.orgpath_bhc_charge_ind,
    case
        when safety_obs_job.provider_job_group_id in (
            'Sitter',
            'psychTech',
            'SafetyObserver')
        then 1 else 0
    end as one_on_one_safety_obs_job_ind,
    case
        when orgpath_safety_obs_ind = 1
            or orgpath_meal_out_of_room_ind = 1
            or (one_on_one_safety_obs_job_ind = 1
                and orgpath_bhc_charge_ind = 0)
        then 1 else 0
    end as safety_obs_record_ind,
    timeorg.timejob_abbreviation,
    case
	when direct.hppd_job_group_id = 'UAP' then 1
	else 0
    end as subtrahend_for_hppd_numerator_ind,
    nursing_pay_period.prior_pay_period_ind,
    nursing_pay_period.future_pay_period_ind
from
    {{ ref('stg_timejob_timeorg') }} as timeorg
    inner join {{ ref('timereport_daily_productive_direct') }} as direct
        on direct.job_organization_id = timeorg.job_organization_id
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on direct.pp_end_dt_key = nursing_pay_period.pp_end_dt_key
    left join {{ ref('stg_nursing_job_code_group') }} as safety_obs_job
        on direct.job_code = safety_obs_job.job_code
where
    safety_obs_record_ind = 1
group by
    direct.worker_id,
    direct.pp_end_dt_key,
    direct.metric_date,
    direct.metric_dt_key,
    direct.company_id,
    direct.cost_center_id,
    direct.cost_center_site_id,
    direct.job_code,
    job_role_grouper,
    direct.hppd_job_group_id,
    direct.job_group_id,
    safety_obs_job.nursing_job_grouper,
    direct.rn_job_ind,
    direct.timereport_paycode,
    direct.overtime_ind,
    direct.nursing_business_report_select_ind,
    direct.cost_center_parent,
    direct.cost_center_type,
    direct.cost_center_group,
    direct.provider_or_other_job_group_id,
    direct.rn_alt_or_other_job_group_id,
    direct.job_organization_id,
    timeorg.orgpath_safety_obs_ind,
    timeorg.orgpath_meal_out_of_room_ind,
    timeorg.orgpath_bhc_charge_ind,
    one_on_one_safety_obs_job_ind,
    safety_obs_record_ind,
    timeorg.timejob_abbreviation,
    nursing_pay_period.prior_pay_period_ind,
    nursing_pay_period.future_pay_period_ind
