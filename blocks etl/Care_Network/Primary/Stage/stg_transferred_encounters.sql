{{ config(meta = {
    'critical': true
}) }}

with employees as (
-- this is needed to join from clarity_emp to worker
-- if there is a X-walk that avoids cdwprd, happy to use it
select
    employee.emp_key,
    employee.prov_key,
    clarity_emp.user_id
from {{ source('cdw', 'employee') }} as employee
    inner join {{ source('clarity_ods', 'clarity_emp') }} as clarity_emp
        on employee.emp_id = clarity_emp.user_id
),
messages as (
select
    stg_encounter.encounter_key,
    stg_encounter.visit_key,
    stg_encounter.csn,
    row_number() over(
        partition by stg_encounter.visit_key
        order by ib_messages.create_time asc)
    as row_num,
    ib_messages.msg_id,
    ib_messages.sender_user_id,
    ib_messages.create_time,
    stg_encounter.department_id as origin_dept_id,
    stg_encounter.department_name as origin_dept_name,
    worker_sender.preferred_reporting_name as sender_emp_nm,
    coalesce(
        prov_sender.provider_type,
        job_code_sender.nursing_default_job_group_id
    ) as sender_emp_type,
    zc_msg_type.name as message_type,
    zc_msg_priority.name as message_priority,
    ib_receiver.status_change_time as status_change_dt,
    101001178 as recipient_dept_id,
    emp_receiver.user_id as recipient_user_id,
    worker_receiver.preferred_reporting_name as status_chg_emp_nm,
    -- some PSR's don't have a provider record,
    -- grabbing the job_group_id for them to fill in PSR as prov_type
    coalesce(
        prov_receiver.provider_type,
        job_code_receiver.nursing_default_job_group_id
    ) as status_chg_emp_type,
    clarity_hip.registry_name,
    clarity_hip.registry_desc,
    ib_receiver.registry_id,
    ib_receiver.recipient_name as recipient_pool_name,
    ib_messages.pool_resp_stat_c,
    -- this will be replaced with a join to zc_pool_resp_stat when in data lake
    case when ib_messages.pool_resp_stat_c = 1 then 'Not Taken'
        when ib_messages.pool_resp_stat_c = 2 then 'Taken'
        else null
    end as pool_resp_status
from {{ ref('stg_encounter') }} as stg_encounter
    inner join {{ source('clarity_ods', 'ib_messages') }} as ib_messages
        on stg_encounter.csn = ib_messages.pat_enc_csn_id
    inner join {{ source('clarity_ods', 'ib_receiver') }} as ib_receiver
        on ib_messages.msg_id = ib_receiver.msg_id
    inner join {{ source('clarity_ods', 'clarity_hip') }} as clarity_hip
        on ib_receiver.registry_id = clarity_hip.registry_id
    left join {{ source('clarity_ods', 'zc_msg_priority') }} as zc_msg_priority
        on ib_messages.msg_priority_c = zc_msg_priority.msg_priority_c
    left join {{ source('clarity_ods', 'zc_msg_type') }} as zc_msg_type
        on ib_messages.msg_type_c = zc_msg_type.msg_type_c
    inner join employees as emp_sender
        on ib_messages.sender_user_id = emp_sender.user_id
    inner join {{ ref('worker') }} as worker_sender
        on emp_sender.emp_key = worker_sender.clarity_emp_key
    left join {{ ref('dim_provider') }} as prov_sender
        on emp_sender.user_id = prov_sender.user_id
    left join {{ ref('job_code_nursing_key_groups') }} as job_code_sender
        on worker_sender.job_code = job_code_sender.job_code
    inner join employees as emp_receiver
        on ib_receiver.status_chg_user_id = emp_receiver.user_id
    inner join {{ ref('worker') }} as worker_receiver
        on emp_receiver.emp_key = worker_receiver.clarity_emp_key
    left join {{ ref('dim_provider') }} as prov_receiver
        on emp_receiver.user_id = prov_receiver.user_id
    left join {{ ref('job_code_nursing_key_groups') }} as job_code_receiver
        on worker_receiver.job_code = job_code_receiver.job_code
where stg_encounter.encounter_date >= '2022-09-12'
    -- exclude encounters already originating in OH or AH nurse triage depts
    and stg_encounter.department_id not in (101001178, 37)
    and ib_receiver.registry_id in (
        50001312, -- OH NJ NURSE TRIAGE POOL
        11499,    -- OH PA 1 NURSE TRIAGE POOL
        11507,    -- OH PA 2 NURSE TRIAGE POOL
        11508     -- OH PHL NURSE TRIAGE POOL
        )
)
select
    encounter_key,
    visit_key,
    csn,
    msg_id as ib_msg_id,
    create_time as msg_create_dt,
    origin_dept_id,
    origin_dept_name,
    sender_user_id,
    sender_emp_nm,
    sender_emp_type,
    recipient_dept_id,
    'PC OFFCE HRS RN TRIAGE' as recipient_dept_name,
    pc_dept.department_display_name as recipient_dept_display_name,
    message_type,
    message_priority,
    status_change_dt,
    recipient_user_id,
    status_chg_emp_nm,
    status_chg_emp_type,
    registry_id,
    registry_name,
    recipient_pool_name,
    pool_resp_status,
    extract(
        epoch from (status_change_dt - msg_create_dt)) / 3600.0
    as transfer_time_hrs
from messages
    inner join {{ ref('lookup_care_network_department_cost_center_sites') }} as pc_dept
        on pc_dept.department_id = messages.recipient_dept_id
where row_num = 1
