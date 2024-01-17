{{ config(meta = {
    'critical': true
}) }}

/* timereport_punch_start_end
capture shift start end time for a worker for a cost center
and adjust timestamps to capture shift hours appropriately
within 20 or 30 minutes so that hourly counts can be
credited
*/
with gather_punch_time as (
select
    dimdt.date_key,
    dimdt.pp_end_dt_key,
    dimdt.pay_period_number,
    dimdt.weekday_name,
    dimdt.holiday_all_employees_ind,
    direct_daily.cost_center_id,
    ts_item.eventdtm as timesheet_date,
    direct_daily.productive_direct_daily_hours as wfctotal_id_hours,
    direct_daily.productive_direct_daily_full_time_percentage as wfctotal_id_fte,
    direct_daily.worker_id,
    direct_daily.job_group_id,
    direct_daily.job_code,
    p_start.punchdtm as start_punch,
    p_end.punchdtm as end_punch,
    case
        when p_start.punchdtm is not null
            and p_end.punchdtm  is not null
        then 1
        end as punch_start_end_ind,
    direct_daily.overtime_ind
from
    {{ ref('timereport_daily_productive_direct') }} as direct_daily
    inner join  {{ ref('stg_date_nursing_pay_period') }} as dimdt
        on direct_daily.metric_date = dimdt.full_date
    left join {{ source('kronos_ods', 'kronos_timesheetitem') }} as ts_item
        on direct_daily.timesheet_item_id = ts_item.timesheetitemid
    left join {{ source('kronos_ods', 'kronos_punchevent') }} as p_start
        on ts_item.startpuncheventid = p_start.puncheventid
    left join {{ source('kronos_ods', 'kronos_punchevent') }} as p_end
        on ts_item.endpuncheventid = p_end.puncheventid
where
    direct_daily.metric_date < current_date - 1
)

select
    date_key,
    pp_end_dt_key,
    pay_period_number,
    weekday_name,
    cost_center_id,
    timesheet_date,
    1 as productive_direct_ind,
    sum(wfctotal_id_hours) as productive_daily_hours,
    sum(wfctotal_id_fte) as productive_daily_full_time_percentage,
    worker_id,
    job_group_id,
    job_code,
    start_punch,
    end_punch,
    coalesce(punch_start_end_ind, 0) as punch_start_end_ind,
    overtime_ind,
    0 as orientation_ind,
    null as indirect_subset,
    case when end_punch is null
    then start_punch
        + cast(productive_daily_hours || ' hours ' as interval)
    end as calc_end_punch,
    coalesce(punch_start_end_ind,
        case
        when calc_end_punch is not null
        then 1 else 0
        end) as can_derive_hours_ind,
    coalesce(end_punch, calc_end_punch) as use_end_punch,
    start_punch - cast(20 || ' minutes ' as interval) as start_window_20_minute_dttm,
    start_punch - cast(30 || ' minutes ' as interval) as start_window_30_minute_dttm,
    use_end_punch - cast(20 || ' minutes ' as interval) as end_window_20_minute_dttm,
    use_end_punch - cast(30 || ' minutes ' as interval) as end_window_30_minute_dttm
from
    gather_punch_time
group by
    date_key,
    pp_end_dt_key,
    pay_period_number,
    weekday_name,
    cost_center_id,
    timesheet_date,
    worker_id,
    job_group_id,
    job_code,
    start_punch,
    end_punch,
    punch_start_end_ind,
    overtime_ind
