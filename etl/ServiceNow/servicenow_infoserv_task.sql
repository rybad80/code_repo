select
    servicenow_task.number as task_number,
    servicenow_ritm.number as request_item_number,
    servicenow_req.number as request_number,
    cast(servicenow_task.opened_at as date) as opened_date,
    servicenow_task.opened_at as opened_timestamp,
    ritm_assignment_group.name as request_item_assignment_group,
    task_assignment_group.name as task_assignment_group,
    servicenow_task.short_description as task_short_description,
    servicenow_task.description as task_description,
    servicenow_ritm.short_description as request_item_short_description,
    worker.preferred_reporting_name as requester_name,
    lower(servicenow_req.sys_created_by) as requester_login,
    worker.job_title as requester_job_title,
    worker.cost_center_name as requester_cost_center,
    servicenow_task.priority,
    case
        when servicenow_task.state = 1 then 'Open'
        when servicenow_task.state = 2 then 'Work In Progress'
        when servicenow_task.state = 3 then 'Closed Complete'
        when servicenow_task.state = 4 then 'Closed Incomplete'
        when servicenow_task.state = 5 then 'Closed Cancelled'
        when servicenow_task.state = 8 then 'Closed Deferred'
        else 'Pending/Other/Unknown'
    end as state_name,
    servicenow_task.state as state_id,
    cast(servicenow_task.closed_at as date) as closed_date,
    servicenow_task.closed_at as closed_timestamp,
    task_assigned_user.name as assigned_user,
    servicenow_task.assigned_to as assigned_user_sys_id,
    servicenow_task.sys_id as task_sys_id,
    case when servicenow_task.assigned_to is not null then 1 else 0 end as assigned_ind,
    case when servicenow_task.closed_at is not null then 1 else 0 end as closed_ind
from
    {{source('servicenow_ods', 'servicenow_task')}} as servicenow_task
    inner join {{source('servicenow_ods', 'servicenow_sys_user_group')}} as task_assignment_group
        on servicenow_task.assignment_group = task_assignment_group.sys_id
      inner join {{source('servicenow_ods', 'servicenow_task')}} as servicenow_ritm
        on servicenow_task.parent = servicenow_ritm.sys_id
          and servicenow_ritm.sys_class_name = 'sc_req_item'
    inner join {{source('servicenow_ods', 'servicenow_sys_user_group')}} as ritm_assignment_group
        on servicenow_ritm.assignment_group = ritm_assignment_group.sys_id
    left join {{source('servicenow_ods', 'servicenow_task')}} as servicenow_req
        on servicenow_ritm.opened_by = servicenow_req.opened_by
        and servicenow_req.sys_class_name = 'sc_request'
        and servicenow_ritm.opened_at between servicenow_req.opened_at - interval '5 seconds'
        and servicenow_req.opened_at + interval '5 seconds'
    left join {{source('servicenow_ods', 'servicenow_sys_user')}} as task_assigned_user
        on servicenow_task.assigned_to = task_assigned_user.sys_id
    left join {{ref('worker')}} as worker
        on lower(servicenow_ritm.sys_created_by) = worker.ad_login
where
     servicenow_task.sys_class_name = 'sc_task'
     and servicenow_ritm.assignment_group in(
        '2582ab121b24601040cbedf8b04bcb27', --'CHQA - Analytics Triage'
        '1c4818cc1b977810ee070dc1604bcb17' --'CHQA - Data Quality'
     )
