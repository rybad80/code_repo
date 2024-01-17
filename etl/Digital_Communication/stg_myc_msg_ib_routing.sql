select
	{{
		dbt_utils.surrogate_key([
			'myc_convo_msgs.thread_id',
			'myc_convo_msgs.line',
			'ib_messages.msg_id',
			'ib_message_thread.thread_id',
            'ib_message_thread.line',
            'ib_message_threa_1.value_line'
		])
    }} as myc_ib_routing_primary_key,
    myc_convo_msgs.thread_id as convo_id,
    myc_convo_msgs.line as convo_line,
    myc_convo_msgs.message_id as myc_message_id,
    ib_messages.msg_id,
    ib_messages.source_msg_id,
    ib_messages.create_time,
    myc_convo_msgs.sent_utc_dttm,
    ib_messages.regarding_topic,
    ib_messages_2.route_hx_hth_id as routing_thread_id,
    ib_message_thread.line as routing_thread_line,
    ib_messages.sender_user_id,
    ib_messages.msg_type_c,
    ib_message_threa_1.value_line,
    ib_message_threa_1.pool_id,
    ib_message_threa_2.recip_id,
    '1' as routing_ind
from
    {{ source('clarity_ods', 'myc_convo_msgs') }} as myc_convo_msgs
    left join {{ source('clarity_ods', 'ib_messages') }} as ib_messages
        on myc_convo_msgs.message_id = ib_messages.mychart_message_id
   left join  {{ source('clarity_ods', 'ib_messages_2') }} as ib_messages_2
        on ib_messages.msg_id = ib_messages_2.msg_id
    left join {{ source('clarity_ods', 'ib_threads') }} as ib_threads
        on ib_messages_2.route_hx_hth_id = ib_threads.thread_id
    left join {{ source('clarity_ods', 'ib_message_thread') }} as ib_message_thread
        on ib_message_thread.message_id = ib_messages.msg_id
    left join {{ source('clarity_ods', 'ib_message_threa_1') }} as ib_message_threa_1
        on ib_message_thread.thread_id = ib_message_threa_1.thread_id
            and ib_message_threa_1.group_line = ib_message_thread.line
    left join {{ source('clarity_ods', 'ib_message_threa_2') }} as ib_message_threa_2
        on ib_message_threa_1.thread_id = ib_message_threa_2.thread_id
            and ib_message_threa_1.group_line = ib_message_threa_2.group_line
            and ib_message_threa_1.value_line = ib_message_threa_2.value_line
where
    ib_messages.create_time > '2022-07-14'
    and ib_messages.status_c != '5' --retracted messages 
    and routing_thread_id is not null
    and ib_threads.type_c in ('2','5') --routing threads and myc_conversation threads