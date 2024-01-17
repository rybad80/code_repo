{{ config(meta = {
    'critical': true
}) }}

/* stg_nccs_hppd_p1_direct_productive
capture just the subset of Acute Care RNs and UAPs'
direct productive time to use for the Hours Per Patient Day metric
for the cost centers NCCS Platform reports on
*/
with select_hppd_src as (
    select
        worker_id,
        pp_end_dt_key,
        metric_date,
        metric_dt_key,
        cost_center_id,
        job_code,
        hppd_job_group_id,
        job_group_id,
        timereport_paycode,
        nursing_business_report_select_ind,
        productive_direct_daily_full_time_percentage,
        productive_direct_daily_hours,
        provider_or_other_job_group_id,
        rn_alt_or_other_job_group_id,
        case hppd_job_group_id
            when 'UAP'
            then hppd_job_group_id
            else 'RN'
        end as grp_for_hppd_abbr
    from
        {{ ref('timereport_daily_productive_direct') }}
    where
        hppd_job_group_id = 'UAP'
        or rn_alt_or_other_job_group_id = 'AcuteRN' -- subset of care RN for HPPD
)

select
    select_hppd_src.worker_id,
    select_hppd_src.pp_end_dt_key,
    select_hppd_src.metric_date,
    select_hppd_src.metric_dt_key,
    select_hppd_src.cost_center_id,
    select_hppd_src.job_code,
    select_hppd_src.hppd_job_group_id,
    select_hppd_src.grp_for_hppd_abbr,
    select_hppd_src.timereport_paycode,
    select_hppd_src.nursing_business_report_select_ind,
    sum(select_hppd_src.productive_direct_daily_full_time_percentage) as hppd_fte,
    sum(select_hppd_src.productive_direct_daily_hours) as hppd_hours,
    select_hppd_src.provider_or_other_job_group_id,
    select_hppd_src.rn_alt_or_other_job_group_id
from
    select_hppd_src
where
    select_hppd_src.nursing_business_report_select_ind = 1
group by
    select_hppd_src.worker_id,
    select_hppd_src.pp_end_dt_key,
    select_hppd_src.metric_date,
    select_hppd_src.metric_dt_key,
    select_hppd_src.cost_center_id,
    select_hppd_src.job_code,
    select_hppd_src.hppd_job_group_id,
    select_hppd_src.grp_for_hppd_abbr,
    select_hppd_src.timereport_paycode,
    select_hppd_src.nursing_business_report_select_ind,
    select_hppd_src.provider_or_other_job_group_id,
    select_hppd_src.rn_alt_or_other_job_group_id
