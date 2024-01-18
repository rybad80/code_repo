{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_extra_pay_p1_daily_worker
from the not productive direct hours, and not direct (thsu indirect or time off) records
pull out any incentive special case records as recorded from Kronos including the indicators
based on rows from lookup_paycode_group_category
--general safety observation per the org path,
--Meals out of Room (MOOR)
--a job code whose job is 100% of direct time as a safety observer
*/

select
    oth_kronos_rec.worker_id,
    oth_kronos_rec.pp_end_dt_key,
    oth_kronos_rec.metric_date,
    oth_kronos_rec.metric_dt_key,
    oth_kronos_rec.company_id,
    oth_kronos_rec.cost_center_id,
    oth_kronos_rec.cost_center_site_id,
    oth_kronos_rec.job_code,
    oth_kronos_rec.hppd_job_group_id,
    oth_kronos_rec.job_group_id,
    oth_kronos_rec.rn_job_ind,
    oth_kronos_rec.timereport_paycode,
    oth_kronos_rec.inducement_category,
    oth_kronos_rec.inducement_ind,
    oth_kronos_rec.other_type,
    oth_kronos_rec.nursing_business_report_select_ind,
    oth_kronos_rec.cost_center_parent,
    oth_kronos_rec.cost_center_type,
    oth_kronos_rec.cost_center_group,
    oth_kronos_rec.provider_or_other_job_group_id,
    oth_kronos_rec.rn_alt_or_other_job_group_id,
    oth_kronos_rec.job_organization_id,
    sum(oth_kronos_rec.worker_daily_total) as extra_pay_hour_plus,
    sum(oth_kronos_rec.money_daily_total) as extra_pay_hour_plus_fte,
    timeorg.orgpath_safety_obs_ind,
    timeorg.orgpath_meal_out_of_room_ind,
    case
        when safety_obs_job.job_code is not null
        then 1 else 0
    end as one_on_one_safety_obs_job_ind,
    case
        when orgpath_safety_obs_ind = 1
            or orgpath_meal_out_of_room_ind = 1
            or one_on_one_safety_obs_job_ind = 1
        then 1 else 0
    end as safety_obs_record_ind
from
    {{ ref('stg_timejob_timeorg') }} as timeorg
    inner join {{ ref('timereport_daily_other') }} as oth_kronos_rec
        on oth_kronos_rec.job_organization_id = timeorg.job_organization_id
    left join {{ ref('job_code_profile') }} as safety_obs_job
        on oth_kronos_rec.job_code = safety_obs_job.job_code
        and safety_obs_job.provider_job_group_id = 'psychTech'
where
    inducement_ind = 1
group by
    oth_kronos_rec.worker_id,
    oth_kronos_rec.pp_end_dt_key,
    oth_kronos_rec.metric_date,
    oth_kronos_rec.metric_dt_key,
    oth_kronos_rec.company_id,
    oth_kronos_rec.cost_center_id,
    oth_kronos_rec.cost_center_site_id,
    oth_kronos_rec.job_code,
    oth_kronos_rec.hppd_job_group_id,
    oth_kronos_rec.job_group_id,
    oth_kronos_rec.rn_job_ind,
    oth_kronos_rec.timereport_paycode,
    oth_kronos_rec.inducement_category,
    oth_kronos_rec.inducement_ind,
    oth_kronos_rec.other_type,
    oth_kronos_rec.nursing_business_report_select_ind,
    oth_kronos_rec.cost_center_parent,
    oth_kronos_rec.cost_center_type,
    oth_kronos_rec.cost_center_group,
    oth_kronos_rec.provider_or_other_job_group_id,
    oth_kronos_rec.rn_alt_or_other_job_group_id,
    oth_kronos_rec.job_organization_id,
    timeorg.orgpath_safety_obs_ind,
    timeorg.orgpath_meal_out_of_room_ind,
    one_on_one_safety_obs_job_ind,
    safety_obs_record_ind
