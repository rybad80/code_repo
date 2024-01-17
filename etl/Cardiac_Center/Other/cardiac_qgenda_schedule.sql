select
     sched.schedulekey as schedule_id,
     sched.date_rw as schedule_date,
     sched.startdate + starttime as start_datetime,
     sched.enddate + endtime as end_datetime,
     replace(replace(regexp_replace(taskabbrev, '[^0-9A-Za-z\-]', ' '), 'â', ' '), '    ', ' - ') as task_abbreviation, -- noqa: L016
     replace(replace(regexp_replace(taskname, '[^0-9A-Za-z\-]', ' '), 'â', ' '), '    ', ' - ') as task_name,
     lastname || ', ' || firstname as staff_full_name,
     firstname as staff_first_name,
     lastname as staff_last_name,
     staff.staffid as staff_id,
     upper(substring(staff.email, 1, instr(staff.email, '@') - 1)) as ad_login
from
     {{source('qgenda_ods','scheduleentry')}} as sched
     inner join {{source('qgenda_ods','staffmember')}} as staff on sched.staffkey = staff.staffkey
where
     sched.compkey = '5b212fe0-1c93-4ac7-b934-561f6e880f58'
     and ispublished = true
     and isstruck = false
