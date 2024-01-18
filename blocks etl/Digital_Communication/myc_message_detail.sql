with emp as (
	select
		employee.emp_key,
		employee.emp_id,
		worker.display_name,
		provider.prov_type,
		worker.position_title,
		worker.job_title,
		case
			when provider.prov_type is null and worker.job_family is null then 'Other Staff'
			when worker.job_family is null then provider.prov_type
			when provider.prov_type is null then position_title
			else worker.job_family
		end as job_family_role,
		worker.job_family,
		worker.job_family_group
	from
		{{ ref('worker') }} as worker
		left join {{ source('cdw', 'employee') }} as employee
			on employee.emp_key = worker.clarity_emp_key
		left join {{ source('cdw', 'provider') }} as provider --noqa: PRS
			on provider.prov_key = worker.prov_key
	where
		worker.termination_date is null or worker.termination_date >= '2021-01-01'
),

convos as (
	select
		stg_myc_convo.convo_id,
		stg_myc_convo.convo_line,
		stg_myc_convo.myc_message_id,
		stg_myc_convo.thread_name,
		stg_myc_convo.parent_thread_id,
		stg_myc_convo.convo_subject,
		stg_myc_convo.subtopic,
		stg_myc_convo.myc_no_reply_c
	from {{ ref('stg_myc_convo') }} as stg_myc_convo
	where
		stg_myc_convo.parent_thread_id is null
		and stg_myc_convo.thread_name not like 'MYC CLONE OF %'
		and stg_myc_convo.convo_subject not like 'RE:%'
)

select
	{{
		dbt_utils.surrogate_key([
			'stg_myc_ib_message.ib_myc_msg_action_key',
			'myc_message.myc_message_id',
			'convos.convo_line',
			'stg_myc_ib_message.ib_cmd_line',
			'stg_myc_ib_message.fwd_line',
			'stg_myc_ib_message.rec_fwd_line'				
		])
    }} as myc_msg_detail_key,
	myc_message.myc_message_id,
	convos.convo_id,
	myc_message.parent_message_id,
	myc_message.pat_enc_csn_id as csn,
	myc_message.pat_id,
	convos.convo_line,
	myc_message.inbasket_msg_id,
	myc_message.message_type,
	myc_message.message_type_id,
	convos.subtopic,
	myc_message.tofrom_pat_c as tofrom_pat_id,
	case
		when myc_message.tofrom_pat_c = '1' then 'To Patient' else 'From Patient'
	end as myc_message_direction,
	myc_message.created_time as myc_message_create_time,
	myc_message.myc_message_sender,
	case
		when myc_message_direction = 'From Patient' then 'Patient'
		else sender.job_family_role
	end as sender_role,
	sender.job_family_role,
	myc_message.myc_message_recipient,
	case
		when myc_message_direction = 'To Patient' then 'Patient'
		else recipient.job_family_role
	end as recipient_role,
	myc_message.department_id,
	myc_message.department_name,
	myc_message.specialty_name,
	myc_message.prov_id,
	myc_message.modified_to,
	myc_message.pool_id,
	clarity_hip.registry_name as pool_name,
	myc_message.subject,
	convos.convo_subject,
	stg_myc_ib_message.ib_cmd_line,
	stg_myc_ib_message.cmd_audit,
	stg_myc_ib_message.command_name,
	stg_myc_ib_message.audit_qa_id as quick_action_id,
	an_hgm_record_info.record_name as quick_action_name,
	stg_myc_ib_message.aud_by_user_id,
	aud_by_user.job_family_role as audit_user_role,
	stg_myc_ib_message.aud_registry,
	stg_myc_ib_message.audit_time,
	stg_myc_ib_message.forward_to_msg_id,
	stg_myc_ib_message.fwd_msg_status_id,
	stg_myc_ib_message.fwd_msg_status,
	stg_myc_ib_message.fwd_line,
	stg_myc_ib_message.rec_fwd_line,
	stg_myc_ib_message.recipient_name as forwarded_to_name,
	stg_myc_ib_message.receive_date,
	stg_myc_ib_message.recipient_emp_id as forward_to_emp_id,
	rec_user.job_family_role as fwd_to_user_role,
	myc_message.not_handled_time,
	myc_message.final_handled_time,
	myc_message.proxy_wpr_id,
	myc_message.first_action_id,
	myc_message.first_action,
	myc_message.first_action_tm,
	myc_message.last_action_id,
	myc_message.last_action,
	myc_message.last_action_tm,
	convos.myc_no_reply_c as myc_no_reply_reason_id,
	myc_message.reply_direct_yn,
	myc_message.notallow_reply_yn,
	max(case
		when stg_myc_ib_message.aud_by_user_id is null
		then 1 else 0 end) over (
			partition by
				myc_message.myc_message_id
	) as resolved_in_pool_ind
from
	{{ ref('stg_myc_message') }} as myc_message
	left join convos
		on myc_message.myc_message_id = convos.myc_message_id
	left join {{ ref('stg_myc_ib_message') }} as stg_myc_ib_message
		on myc_message.myc_message_id = stg_myc_ib_message.mychart_message_id
		and myc_message.inbasket_msg_id = stg_myc_ib_message.msg_id
	left join emp as sender
		on myc_message.myc_message_sender = sender.emp_id
	left join emp as recipient
		on myc_message.myc_message_recipient = recipient.emp_id
	left join emp as aud_by_user
		on stg_myc_ib_message.aud_by_user_id = aud_by_user.emp_id
	left join {{ source('clarity_ods', 'clarity_hip') }} as clarity_hip
		on myc_message.pool_id = clarity_hip.registry_id
	left join emp as fwd_user
		on stg_myc_ib_message.forward_by_user_id = fwd_user.emp_id
	left join emp as rec_user
		on stg_myc_ib_message.recipient_emp_id = rec_user.emp_id
	left join {{source('clarity_ods', 'an_hgm_record_info')}} as an_hgm_record_info
		on stg_myc_ib_message.audit_qa_id = an_hgm_record_info.record_id
