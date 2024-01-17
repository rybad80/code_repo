select
    ib_messages.mychart_message_id,
	ib_messages.msg_id,
	ib_messages.msg_type_c,
	ib_messages.sender_user_id,
	ib_messages.create_time,
	ib_messages.source_msg_id as parent_ib_msg,
    ibm_forward.status_c as fwd_msg_status_id,
	ib_forward_items.forward_to_msg_id,
	ib_forward_items.line as fwd_line,
    ib_forward_items.forward_by_user_id,
	ib_forward_items.forwarded_time
from
	{{ source('clarity_ods', 'ib_messages') }} as ib_messages
    left join {{ source('clarity_ods', 'ib_forward_items') }} as ib_forward_items
        on ib_messages.msg_id = ib_forward_items.msg_id
    left join {{ source('clarity_ods', 'ib_messages') }} as ibm_forward
		on ib_forward_items.forward_to_msg_id = ibm_forward.msg_id
where
	ib_messages.mychart_message_id is not null --only need myc messages
	and ib_messages.create_time >= '2022-07-14'
