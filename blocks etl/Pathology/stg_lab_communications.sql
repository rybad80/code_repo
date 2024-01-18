{{ config(materialized='table', dist='comm_cmt_note_id') }}

select
    cal_reference_crm.comm_id as call_communication_id,
    cust_service.comm_id as communication_id,
    cust_service.lab_specimen_id as specimen_id,
    cust_service.lab_test_id as test_id,
    cust_service.lab_order_id as procedure_order_id,
    zc_lab_call_topic.name as lab_call_topic,
    date(cust_service.entry_date) as entry_date,
    cast(timezone(cal_comm_tracking.comm_instant_dttm, 'GMT', 'America/New_York') as datetime)
        as communication_instant_datetime,
    cast(timezone(cal_comm_tracking.update_instant_dttm, 'GMT', 'America/New_York') as datetime)
        as update_instant_datetime,
    cast(timezone(max(cal_comm_tracking.update_instant_dttm) over (partition by (cust_service.lab_order_id)),
        'GMT', 'America/New_York') as datetime) as max_communication_datetime,
    cal_comm_tracking.caller_name as contact_name,
    clarity_emp.user_id as communication_user_id,
    clarity_emp.name as communication_user_name,
    cal_comm_tracking.phone_number,
    cal_comm_tracking.comm_cmt_note_id
from
    {{source('clarity_ods', 'cust_service')}} as cust_service
    inner join {{source('clarity_ods', 'cal_reference_crm')}} as cal_reference_crm
        on cust_service.comm_id = cal_reference_crm.ref_crm_id
    inner join {{source('clarity_ods', 'cal_comm_tracking')}} as cal_comm_tracking
        on cal_reference_crm.comm_id = cal_comm_tracking.comm_id
    inner join {{source('clarity_ods', 'zc_lab_call_topic')}} as zc_lab_call_topic
        on cal_comm_tracking.lab_call_topic_c = zc_lab_call_topic.lab_call_topic_c
    inner join {{source('clarity_ods', 'zc_comm_type')}} as zc_comm_type
        on cal_comm_tracking.comm_type_c = zc_comm_type.comm_type_c
    inner join {{source('clarity_ods', 'clarity_emp')}} as clarity_emp
        on cal_comm_tracking.user_id = clarity_emp.user_id
    inner join {{source('clarity_ods', 'zc_call_type')}} as zc_call_type
        on cal_comm_tracking.call_type_c = zc_call_type.call_type_c
where cust_service.lab_specimen_id is not null
