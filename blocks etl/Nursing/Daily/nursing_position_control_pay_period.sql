{{ config(meta = {
    'critical': true
}) }}

/* nursing_position_control_pay_period
rolling the nursing segment data out to applicable pay periods
for the appropriate rolling window
*/

with min_pp as (
    select
        min(pp_end_dt_key) as min_pp_end_dt_key
    from
        {{ ref('nursing_pay_period') }}
    where
        nccs_platform_window_ind = 1
),

pay_periods_to_process as (
    select
        nursing_pay_period.pp_end_dt_key as end_dt_key,
        nursing_pay_period.fiscal_year,
        nursing_pay_period.pay_period_number,
        nursing_pay_period.pp_start_dt_key,
        nursing_pay_period.pp_start_dt,
        nursing_pay_period.pp_end_dt
    from {{ ref('nursing_pay_period') }} as nursing_pay_period
        inner join min_pp
    on
        nursing_pay_period.pp_end_dt_key >= min_pp.min_pp_end_dt_key
)

select
    nursing_position_control_segment.emp_number as worker_id,
    pay_periods_to_process.end_dt_key as pp_dt_key,
    nursing_position_control_segment.fte_status,
    nursing_position_control_segment.new_fte_status,
    pay_periods_to_process.pp_start_dt,
    pay_periods_to_process.pp_end_dt,
    coalesce(nursing_position_control_segment.new_fte_status,
            nursing_position_control_segment.fte_status) as current_fte,
    case when pay_periods_to_process.pp_end_dt < nursing_position_control_segment.start_on_unit_job
        or pay_periods_to_process.pp_end_dt < nursing_position_control_segment.orientation_start_date
        or (pay_periods_to_process.pp_end_dt >= nursing_position_control_segment.orientation_start_date
        and pay_periods_to_process.pp_end_dt <= nursing_position_control_segment.orientation_end_date)
        or (pay_periods_to_process.pp_end_dt >= nursing_position_control_segment.loa_start_date
        and pay_periods_to_process.pp_end_dt <= nursing_position_control_segment.loa_end_date)
        or pay_periods_to_process.pp_end_dt >= nursing_position_control_segment.term_date then '0'
    else nursing_position_control_segment.fte_status end as fte,
    case when (pay_periods_to_process.pp_end_dt < nursing_position_control_segment.start_on_unit_job
        or pay_periods_to_process.pp_end_dt >= nursing_position_control_segment.term_date) then '0'
        else fte_status end as hired_fte,
    case when (pay_periods_to_process.pp_end_dt < nursing_position_control_segment.start_on_unit_job
        or pay_periods_to_process.pp_end_dt >= nursing_position_control_segment.term_date) then '0'
        else case when nursing_position_control_segment.fte_status::float > 0
            then 1 else 0 end end as headcount_ind,
    case when nursing_position_control_segment.term_date
        is not null then 'termed record'
        else 'active record' end as active_record,
    nursing_position_control_segment.cost_center as cost_center_cd,
    nursing_position_control_segment.job_code,
    nursing_position_control_segment.start_on_unit_job,
    nursing_position_control_segment.new_job_start_date,
    nursing_position_control_segment.orientation_start_date,
    nursing_position_control_segment.orientation_end_date,
    nursing_position_control_segment.loa_start_date,
    nursing_position_control_segment.loa_end_date,
    nursing_position_control_segment.end_date_on_unit,
    nursing_position_control_segment.term_date,
    trim(nursing_position_control_segment.full_name) as full_name
from {{source('manual_ods_nccs', 'nursing_position_control_segment')}} as nursing_position_control_segment
cross join pay_periods_to_process
