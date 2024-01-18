select
	{{
		dbt_utils.surrogate_key([
			'stg_myc_ib_message_raw.msg_id',
			'ib_command_audit.line',
			'stg_myc_ib_message_raw.fwd_line',
			'ib_receiver.line'
		])
    }} as ib_myc_msg_action_key,
	stg_myc_ib_message_raw.mychart_message_id,
	stg_myc_ib_message_raw.msg_id,
	stg_myc_ib_message_raw.msg_type_c,
	stg_myc_ib_message_raw.sender_user_id,
	stg_myc_ib_message_raw.create_time,
	stg_myc_ib_message_raw.parent_ib_msg,
	ib_command_audit.line as ib_cmd_line,
	ib_command_audit.cmd_audit,
	ib_commands.command_name,
	ib_command_audit.aud_by_user_id,
	ib_command_audit.aud_registry,
	ib_command_audit.aud_membership,
	ib_command_audit.audit_qa_id,
	ib_command_audit.audit_time,
	stg_myc_ib_message_raw.forward_to_msg_id,
	stg_myc_ib_message_raw.fwd_msg_status_id,
	case
		when fwd_msg_status_id = '1' then 'Create'
		when fwd_msg_status_id = '2' then 'Sent'
		when fwd_msg_status_id = '3' then 'Pend'
		when fwd_msg_status_id = '4' then 'Done'
		when fwd_msg_status_id = '5' then 'Retract'
		when fwd_msg_status_id = '6' then 'Edit'
	end as fwd_msg_status,
	stg_myc_ib_message_raw.fwd_line,
	ib_receiver.line as rec_fwd_line,
	stg_myc_ib_message_raw.forward_by_user_id,
	stg_myc_ib_message_raw.forwarded_time,
	ib_receiver.registry_id, --can be fowarded TO multiple registries 
	ib_receiver.recipient_name,
	ib_receiver.receive_date,
	ib_receiver.recipient_emp_id
from
	{{ ref('stg_myc_ib_message_raw') }} as stg_myc_ib_message_raw
    left join {{ source('clarity_ods', 'ib_command_audit') }} as ib_command_audit
        on stg_myc_ib_message_raw.msg_id = ib_command_audit.msg_id
        and ib_command_audit.cmd_audit in ('106', '163')
		and ib_command_audit.cmd_audit is not null
	left join {{ source('clarity_ods', 'ib_commands') }} as ib_commands
        on ib_command_audit.cmd_audit = ib_commands.command_id
    left join {{ source('clarity_ods', 'ib_receiver') }} as ib_receiver
		on stg_myc_ib_message_raw.forward_to_msg_id = ib_receiver.msg_id
