{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_non_direct_p1_pp_hours
capture the subset of hours aggregated to pay period of all job groups/pay codes
for the cost centers NCCS Platform reports on (and add nursing_job_rollup (job group level 4)
mainly for the staff nurse and UAP roll-ups) to summarize the
-- indirect productive time (Orientation, Conference etc) and
-- time off (non-productive: PPL, holiday, unplanned UPPL, Jury etc)
*/
with ambulatory_rn_job as (
    select
        job_code as ambulatory_rn_job_code,
        1 as ambulatory_rn_job_ind
    from
        {{ ref('job_code_job_group_link') }}
    where
        job_group_id = 'AmbulatoryRN'
    group by
        job_code
),

non_direct_worker_job_group as (
    select
        non_direct_src.worker_id,
        non_direct_src.pp_end_dt_key,
        non_direct_src.cost_center_id,
        non_direct_src.job_group_id,
        non_direct_src.rn_job_ind,
        coalesce(ambulatory_rn_job.ambulatory_rn_job_ind, 0) as ambulatory_rn_job_ind,
        staff_nurse_etc.staff_nurse_ind,
        staff_nurse_etc.nursing_job_rollup,
        non_direct_src.timereport_paycode,
        non_direct_src.non_direct_subset,
        non_direct_src.non_productive_ind,
        non_direct_src.nursing_business_report_select_ind,
        non_direct_src.non_direct_daily_full_time_percentage,
        non_direct_src.non_direct_daily_hours
    from
        {{ ref('timereport_daily_non_direct') }} as non_direct_src
        left join {{ ref('job_group_levels_nursing') }} as staff_nurse_etc
            on non_direct_src.job_group_id = staff_nurse_etc.job_group_id
        left join ambulatory_rn_job
            on non_direct_src.job_code = ambulatory_rn_job.ambulatory_rn_job_code
    where
        non_direct_src.nursing_business_report_select_ind = 1
)

select
    nursing_pay_period.prior_pay_period_ind,
    non_direct_worker_job_group.pp_end_dt_key,
    non_direct_worker_job_group.cost_center_id,
    non_direct_worker_job_group.job_group_id,
    non_direct_worker_job_group.rn_job_ind,
    non_direct_worker_job_group.ambulatory_rn_job_ind,
    non_direct_worker_job_group.staff_nurse_ind,
    non_direct_worker_job_group.nursing_job_rollup,
    non_direct_worker_job_group.timereport_paycode,
    non_direct_worker_job_group.non_direct_subset,
    non_direct_worker_job_group.non_productive_ind,
    non_direct_worker_job_group.nursing_business_report_select_ind,
    sum(non_direct_worker_job_group.non_direct_daily_full_time_percentage) as non_direct_fte,
    sum(non_direct_worker_job_group.non_direct_daily_hours) as non_direct_hours
from
    non_direct_worker_job_group
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on non_direct_worker_job_group.pp_end_dt_key = nursing_pay_period.pp_end_dt_key
group by
    nursing_pay_period.prior_pay_period_ind,
    non_direct_worker_job_group.pp_end_dt_key,
    non_direct_worker_job_group.cost_center_id,
    non_direct_worker_job_group.job_group_id,
    non_direct_worker_job_group.rn_job_ind,
    non_direct_worker_job_group.ambulatory_rn_job_ind,
    non_direct_worker_job_group.staff_nurse_ind,
    non_direct_worker_job_group.nursing_job_rollup,
    non_direct_worker_job_group.timereport_paycode,
    non_direct_worker_job_group.non_direct_subset,
    non_direct_worker_job_group.non_productive_ind,
    non_direct_worker_job_group.nursing_business_report_select_ind
