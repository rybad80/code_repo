{{ config(meta = {
    'critical': true
}) }}

select
    count(distinct invited_user_id) as invited_user_count,
    count(
        distinct case when red_flag in (0, 1, 2, 3, 4, 5) then employee_id end
    ) as registered_emp_count,
    count(
        distinct case when red_flag in (0, 1, 2, 3, 4) then employee_id end
    ) as checkedin_emp_count,
    sum(checkin_last_24_hr_ind) as last24hr_checkin_total,
    case when sum(checkin_last_24_to_48_ind) = 0 then null
        else  (
       (last24hr_checkin_total / sum(checkin_last_24_to_48_ind)) - 1
    ) * 100 end as volume_change_percent_24hr,
    -- volume change percentage from prior 24 hours
    sum(
        case when (checkin_last_24_hr_ind = 1 and red_flag = 1) then 1 else 0 end
    ) as last24hr_redctcflag_count,
    sum(
        case when (checkin_last_24_hr_ind = 1 and red_flag = 2) then 1 else 0 end
    ) as last24hr_redmanagerflag_count,
    sum(
        case when (checkin_last_24_hr_ind = 1 and red_flag = 3) then 1 else 0 end
    ) as last24hr_yellowflag_count,
    sum(
        case when (checkin_last_24_hr_ind = 1 and red_flag = 4) then 1 else 0 end
    ) as last24hr_greyflag_count
from
    {{ref('stg_outbreak_daily_checkin')}}
