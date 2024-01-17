with final_case_date as ( -- region final scheduled date for each case
--granularity: surgical case
    select
    or_case_audit_history.or_case_key,
    max(or_case_audit_history.case_resched_dt) as case_resched_date,
    row_number() over(
        partition by or_case_audit_history.or_case_key order by or_case_audit_history.or_case_key
    ) as row_check

    from
    {{ source('cdw', 'or_case_audit_history') }} as or_case_audit_history

    where
    or_case_audit_history.case_resched_dt is not null

    group by
    or_case_audit_history.or_case_key
--end region
),

removals as (--region surgical case removals
    select
    fact_periop.posted_ind,
    or_case_audit_history.or_case_key,
    or_case.or_case_id,
    patient.full_nm,
    patient.pat_mrn_id,
    location.loc_nm,
    dict_svc.dict_nm as service,
    or_case_audit_history.audit_act_dt as removal_date, --date it was removed from schedule
    dim_or_cancel_reason.or_cancel_rsn_nm,
    or_case_audit_history.cancel_case_cmt,
    or_case_audit_history.case_unsched_dt, --date it was unscheduled from
    dict_status.dict_nm as status,
    or_case_audit_history.case_resched_dt, --if rescheduled, date case was rescheduled to
    --case can be removed and resched multiple times, this returns the most recent date case was rescheduled to
    case
        when lower(status) = 'scheduled' then final_case_date.case_resched_date else null
    end as latest_resched_dt,
    case
        when
            lower(
                status
            ) = 'scheduled' and latest_resched_dt is not null then days_between(
                date(latest_resched_dt), date(case_unsched_dt)
            )
        else null
    end as lead_time,
    --1 will result in most recent removal date
    row_number() over(
        partition by or_case_audit_history.or_case_key order by or_case_audit_history.audit_act_dt desc
    ) as removal_num,
    case
        when lower(dim_or_cancel_reason.or_cancel_rsn_nm) like '%other: public health emergency%' then 1
        when lower(cancel_case_cmt) like '%other: public health emergency%' then 1
        when lower(cancel_case_cmt) like '%covid%' then 1
        when lower(cancel_case_cmt) like '%convid%' then 1
        when lower(cancel_case_cmt) like '%covod%' then 1
        when lower(cancel_case_cmt) like '%cov 19%' then 1
        when lower(cancel_case_cmt) like '%cov19%' then 1
        when lower(cancel_case_cmt) like '%coronavirus%' then 1
        when lower(cancel_case_cmt) like '%virus%' then 1
        when lower(cancel_case_cmt) like '%restriction%' then 1
        when lower(cancel_case_cmt) like '%restricition%' then 1
        when lower(cancel_case_cmt) like '%per chop%' then 1
        when lower(cancel_case_cmt) like '%chop cx%' then 1
        when lower(cancel_case_cmt) like '%or closed%' then 1
        when lower(cancel_case_cmt) like '%non urgent or%' then 1
        else 0 end as covid_removal_ind,
    case when latest_resched_dt is not null then 1 else 0 end as resched_ind,
    --flags if *any* removal action was due to covid
    max(
        case when covid_removal_ind = 1 then 1 else 0 end
    ) over(partition by or_case.or_case_key order by or_case.or_case_key) as any_covid_removal_ind

    from
    {{ source('cdw', 'or_case_audit_history') }} as or_case_audit_history
    inner join {{ source('cdw', 'dim_or_cancel_reason') }} as dim_or_cancel_reason
            on dim_or_cancel_reason.dim_or_cancel_rsn_key = or_case_audit_history.dim_or_cancel_rsn_key
    inner join {{ source('cdw', 'or_case') }} as or_case
            on or_case_audit_history.or_case_key = or_case.or_case_key
    inner join {{ source('cdw', 'location') }} as location --noqa: L029
        on location.loc_key = or_case.loc_key
    inner join {{ source('cdw', 'cdw_dictionary') }} as dict_status
        on dict_status.dict_key = or_case.dict_or_sched_stat_key
    inner join {{ source('cdw', 'cdw_dictionary') }} as dict_svc
        on dict_svc.dict_key = or_case.dict_or_svc_key
    inner join {{ source('cdw', 'patient') }} as patient
        on patient.pat_key = or_case.pat_key
    left join {{ source('cdw', 'or_log') }} as or_log
        on or_log.case_key = or_case_audit_history.or_case_key
    left join {{ ref('fact_periop') }} as fact_periop
        on fact_periop.log_key = or_log.log_key
    left join final_case_date
        on final_case_date.or_case_key = or_case.or_case_key

    where
    -- "removed" this is the ID assigned to the act of a scheduler removing a case from the schedule
    or_case_audit_history.dim_or_audit_act_key = 218

    /*or_cancel_rsn_nm is an optional filter:
    once a case is scheduled, the only way to make certain modifications to it
    (like change a procedure or a room) is to remove it from the schedule, make the change, then re-schedule it
    this can make it seem like the case was removed when it was really just a modification to it.
    this filters remove cases removed for modificiations / errors
    note: shuffle is used when a scheduler is on the phone w/ family and e-scheduling in real time
    (previously this was being filtered out)
    --'shuffle'
    --,'not applicable'
    */
    and lower(dim_or_cancel_reason.or_cancel_rsn_nm) not in (
                                                            'rearrange cases',
                                                            'duplicate case'
                                                            )

    /*-- if a case is cancelled and rescheudled and is completed,
    the "cancel" designation is still attached to the case.
    this filter removes the cases that were cancelled, rescheduled but ultimately completed
    */
    and lower(dict_status.dict_nm) != 'completed'
    and date(or_case_audit_history.audit_act_dt) >= '2020-03-01'
    -- end region
)

select *

from removals

--where removal_num = 1
