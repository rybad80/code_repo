{{ config(meta = {
    'critical': false
}) }}

with messages as (
	select
		myc_mesg.message_id as myc_message_id,
		myc_mesg.inbasket_msg_id
	from {{ source('clarity_ods', 'myc_mesg') }} as myc_mesg
	--messages from patients, epic encounter fix date
	where
		myc_mesg.tofrom_pat_c = '2'
		and myc_mesg.created_time >= '2022-07-14'
		and myc_mesg.myc_msg_typ_c != '999' --exclude system generated messages 
),

quick_actions as (
	select
		msg_id as quick_action_msg_id,
		line,
		aud_by_user_id as qa_user_id,
		aud_registry as pool_id,
		registry_name as pool_name,
		command_id,
		command_name,
		record_id as quick_action_id,
		record_name as quick_action_name
	from
		{{ source('clarity_ods', 'ib_command_audit') }} as msg
	inner join {{ source('clarity_ods', 'ib_commands') }} as ib_commands
		on msg.cmd_audit = ib_commands.command_id
	inner join {{ source('clarity_ods', 'an_hgm_record_info') }} as an_hgm_record_info
     on msg.audit_qa_id = an_hgm_record_info.record_id
	inner join {{ source('clarity_ods', 'clarity_hip') }} as reg on msg.aud_registry = reg.registry_id
	where
		audit_time >= '2022-07-14'
		and command_name = 'REPLY TO PATIENT'
		and record_name in ('Appt Needed', 'Call Needed', 'Reply, Done')
),

replies as (
	select
		myc_mesg.message_id as reply_message_id,
		myc_mesg.notallow_reply_yn,
		myc_mesg.reply_direct_yn,
		myc_mesg.parent_message_id
	from
		{{ source('clarity_ods', 'myc_mesg') }} as myc_mesg
	where
		myc_mesg.parent_message_id is not null
		and myc_mesg.created_time >= '2022-07-14'
		and myc_mesg.tofrom_pat_c = '1'
)

select
	{{
		dbt_utils.surrogate_key([
			'quick_actions.quick_action_msg_id',
			'quick_actions.line'
		])
	}} as myc_quick_action_key,
	messages.*,
	quick_actions.*,
	replies.*
from
	messages
	left join quick_actions on messages.inbasket_msg_id = quick_actions.quick_action_msg_id
	left join replies on  messages.myc_message_id = replies.parent_message_id
where
	quick_actions.command_name is not null
