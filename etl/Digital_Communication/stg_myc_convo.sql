select
    {{
        dbt_utils.surrogate_key([
                'myc_convo_msgs.thread_id',
                'myc_convo_msgs.message_id',
                'myc_convo_msgs.line'
        ])
    }} as myc_msgs_convo_key,
	myc_convo_msgs.thread_id as convo_id,
	myc_convo_msgs.line as convo_line,
	myc_convo_msgs.message_id as myc_message_id,
	myc_convo.thread_name,
	myc_convo.parent_thread_id,
	myc_convo.subject as convo_subject,
	coalesce(cust_svc.topic_display_name, med_advice.topic_display_name) as subtopic,
	myc_convo.myc_no_reply_c
from
    {{ source('clarity_ods', 'myc_convo_msgs') }} as myc_convo_msgs
    left join {{ source('clarity_ods', 'myc_convo') }} as myc_convo
        on myc_convo_msgs.thread_id = myc_convo.thread_id
    left join {{ source('clarity_ods', 'myc_convo_abt_med_advice') }} as med_advice
        on myc_convo.thread_id = med_advice.thread_id
    left join {{ source('clarity_ods', 'myc_convo_abt_cust_svc') }}  as cust_svc
        on myc_convo.thread_id = cust_svc.thread_id
where
    myc_convo_msgs.sent_utc_dttm >= '2022-07-14'
