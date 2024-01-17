with first_sched as (
    select
        surgery_encounter.log_key,
        min(or_case_audit_history.audit_act_dt) as first_booked_date
    from
        {{ref('surgery_encounter')}} as surgery_encounter
        inner join {{source('cdw', 'or_log')}} as orl
            on orl.log_id = surgery_encounter.log_id
        inner join {{source('cdw', 'or_case')}} as orc
            on orc.log_key = orl.log_key
        inner join {{source('cdw', 'or_case_audit_history')}} as or_case_audit_history
            on or_case_audit_history.or_case_key = orc.or_case_key
        inner join {{source('cdw', 'dim_or_audit_action')}} as dim_or_audit_action
            on dim_or_audit_action.dim_or_audit_act_key = or_case_audit_history.dim_or_audit_act_key
    where
        lower(or_audit_act_nm) = 'scheduled'
    group by
        surgery_encounter.log_key
),
timestamps as (
    select
        surgery_encounter.or_key,
        min(case when d_case_times.src_id = 5 then event_in_dt end) as in_room_date
    -- wheels into the or
    from
        {{ref('surgery_encounter')}} as surgery_encounter
        left join {{source('cdw', 'or_log_case_times')}} as or_log_case_times
            on or_log_case_times.log_key = surgery_encounter.or_key
        inner join {{source('cdw', 'cdw_dictionary')}} as d_case_times
            on d_case_times.dict_key = or_log_case_times.dict_or_pat_event_key
    group by
        surgery_encounter.or_key
)
select
    surgery_date,
    surgery_encounter.visit_key,
    surgery_encounter.location_group,
    surgery_encounter.log_key,
    location as location_name,
    case
        when date(timestamps.in_room_date) - 1 = date(first_sched.first_booked_date)
            and cast(to_char(first_sched.first_booked_date, 'hh24:mi:ss') as time)
            > cast('09:00:00' as time) then 1
        when date(timestamps.in_room_date) = date(first_sched.first_booked_date) then 1
        else 0
    end as add_on_ind,
    row_number() over (
        partition by surgery_encounter.visit_key
        order by surgery_date asc
    ) as surg_num_per_visit
from
    {{ref('surgery_encounter')}} as surgery_encounter
    inner join timestamps
        on timestamps.or_key = surgery_encounter.or_key
    left join first_sched
        on first_sched.log_key = surgery_encounter.or_key
where
    lower(case_status) = 'completed'
    and surgery_date > '2019-01-01'
    and lower(surgery_encounter.location) in (
        'cardiac operative imaging complex',
        'periop complex',
        'voorhees day surgery',
        'king of prussia day surgery',
        'king of prussia hospital',
        'bucks day surgery',
        'brandywine valley day surgery'
    )
