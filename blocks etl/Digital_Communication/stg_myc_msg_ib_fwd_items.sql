select
	{{
		dbt_utils.surrogate_key([
			'ib_messages.msg_id',
            'ib_forward_items.line',
			'ib_receiver.line'
		])
    }} as myc_ib_fwd_primary_key,
    --this should connect to the ib_message associated with the myc_message
    ib_messages.msg_id,
    ib_forward_items.line as fwd_line,
    ib_forward_items.forward_to_msg_id,
    ib_forward_items.forward_by_user_id,
    ib_forward_items.forwarded_time,
    ib_receiver.msg_id as rec_msg_id,
    ib_receiver.line as rec_line,
    ib_receiver.recipient_name,
    ib_receiver.registry_id,
    ib_receiver.recipient,
    '1' as forwarding_ind
from
    {{ source('clarity_ods', 'ib_messages') }} as ib_messages
    left join {{ source('clarity_ods', 'ib_forward_items') }} as ib_forward_items
        on ib_messages.msg_id = ib_forward_items.msg_id
    left join {{ source('clarity_ods', 'ib_receiver') }} as ib_receiver
        on ib_forward_items.forward_to_msg_id = ib_receiver.msg_id
where
    ib_messages.mychart_message_id is not null
    and ib_messages.source_msg_id is not null 
    and ib_messages.status_c != '5' --retracted messages 
    and ib_messages.create_time > '2022-07-14'
    and ib_receiver.recipient_name is not null 
