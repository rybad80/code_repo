select
    schedule_entry.schedulekey,
    schedule_entry.staffkey,
    schedule_entry.staffabbrev,
    schedule_entry.staffemail,
    schedule_entry.staffid,
    schedule_entry.startdate,
    schedule_entry.starttime,
    schedule_entry.taskkey,
    schedule_entry.taskname,
    schedule_entry.locationname,
    schedule_entry.lastmodifieddateutc,
    schedule_entry.upd_dt
from {{ source('qgenda_ods', 'scheduleentry') }} as schedule_entry
where schedule_entry.isstruck = 'False'
and schedule_entry.tasktype = 'Working'
and schedule_entry.compkey = '1121178a-aa59-4654-9160-043975c9fff1' -- SCC RAM CompanyKey
and schedule_entry.startdate > '2020-07-01'
