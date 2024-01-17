select
    fact_periop.log_id,
    -- only the proceure length (no setup/clenaup)
    or_case.tot_tm_needed - or_case.setup_offset - or_case.clnup_offset as sched_proc_lgth,
    or_case.setup_offset,
    or_case.clnup_offset,
    or_case.tot_tm_needed as sched_case_lgth, -- setup length + procedure length + cleanup length
    or_case.case_begin_dt, -- case_begin_dt + setup_offset = sched_start_time
    or_case.case_begin_dt + interval(or_case.setup_offset || 'minutes') as sched_start_time,
    times.in_room as actual_start_time,
    extract(epoch from times.in_room - sched_start_time) / 60 as mins_late,
    case when mins_late > 0 then 1 else 0 end as late_start_ind,
    or_case.case_end_dt - interval(or_case.clnup_offset || 'minutes') as sched_end_time,
    times.out_room as actual_end_time,
    extract(epoch from actual_end_time - sched_end_time) / 60 as mins_overrun,
    case when mins_overrun > 0 then 1 else 0 end as late_finish_ind,
    extract(epoch from times.out_room - times.in_room) / 60 as actual_proc_lgth,
    actual_proc_lgth - sched_proc_lgth as mins_deviated

from {{ ref('fact_periop') }} as fact_periop
    inner join {{ source('cdw', 'or_log') }} as or_log
        on or_log.log_id = fact_periop.log_id
    inner join {{ source('cdw', 'or_case') }} as or_case
        on or_case.or_case_key = or_log.case_key
    inner join {{ ref('fact_periop_timestamps') }} as times on times.log_id = or_log.log_id
