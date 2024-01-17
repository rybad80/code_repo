{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_direct_p2_pp_subset
capture the total direct and Per Diem and Overtime subsets of
productive direct (regular and overtime hours worked) hours
aggreagated to the cost center and job group for the pay period
*/
with
direct_pay_period_hours as (
    select
        prior_pay_period_ind,
        metric_dt_key,
        cost_center_id,
        job_code,
        job_group_id,
        pay_code,
        pp_sum_hours
    from
        {{ ref('stg_nursing_direct_p1_pp_hours') }}
),

unit_per_diem_job as (
    select
        job_code as unit_per_diem_job_code,
        1 as per_diem_rn_job_ind
    from
        {{ ref('job_code_job_group_link') }}
    where
        job_group_id = 'UnitPerDiemNurse'
    group by
        job_code
),

amb_rn_job as (
    select
        job_code as amb_rn_job_code,
        1 as amb_rn_job_ind
    from
        {{ ref('job_code_job_group_link') }}
    where
        job_group_id = 'AmbulatoryRN'
    group by
        job_code
),

set_subset_hours as (
    select
        direct_pay_period_hours.prior_pay_period_ind,
        direct_pay_period_hours.metric_dt_key,
        direct_pay_period_hours.cost_center_id,
        direct_pay_period_hours.job_group_id,
        direct_pay_period_hours.pp_sum_hours,
        case
            when direct_pay_period_hours.pay_code = 'OVERTIME'
            then direct_pay_period_hours.pp_sum_hours
            else 0
            end as overtime_hours,
        case
            when unit_per_diem_job.per_diem_rn_job_ind = 1
            then direct_pay_period_hours.pp_sum_hours
            else 0
            end as per_diem_rn_hours,
        case
            when amb_rn_job.amb_rn_job_ind = 1
            then direct_pay_period_hours.pp_sum_hours
            else 0
            end as aggregate_ambulatory_rn_hours
    from
        direct_pay_period_hours
        left join unit_per_diem_job
            on direct_pay_period_hours.job_code = unit_per_diem_job.unit_per_diem_job_code
        left join amb_rn_job
            on direct_pay_period_hours.job_code = amb_rn_job.amb_rn_job_code
)

select
    prior_pay_period_ind,
    metric_dt_key,
    cost_center_id,
    job_group_id,
    sum(pp_sum_hours) as aggregate_pp_sum_hours,
    sum(overtime_hours) as aggregate_overtime_hours,
    sum(per_diem_rn_hours) as aggregate_per_diem_rn_hours,
    sum(aggregate_ambulatory_rn_hours) as aggregate_ambulatory_rn_hours
from
    set_subset_hours

group by
    prior_pay_period_ind,
    metric_dt_key,
    cost_center_id,
    job_group_id
