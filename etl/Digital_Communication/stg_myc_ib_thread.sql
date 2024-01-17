with threads as (
	select
		ib_message_thread.thread_id,
		ib_message_thread.message_id,
		max(ib_message_threa_1.value_line) as recip_max,
		max(ib_message_threa_2.value_line) as pool_max
	from
        {{source('clarity_ods', 'ib_message_thread')}} as ib_message_thread
		left join {{source('clarity_ods', 'ib_message_threa_1')}} as ib_message_threa_1
			on ib_message_thread.thread_id = ib_message_threa_1.thread_id
				and ib_message_thread.line = ib_message_threa_1.group_line
		left join {{source('clarity_ods', 'ib_message_threa_2')}} as ib_message_threa_2
			on ib_message_thread.thread_id = ib_message_threa_2.thread_id
				and ib_message_thread.line = ib_message_threa_2.group_line
	where
		exists (select
					thread_id
				from
					{{source('clarity_ods', 'ib_threads')}} as ib_threads
				where
					ib_threads.thread_id = ib_message_thread.thread_id
				)
		and ib_message_thread.instant_sent_tm >= '2022-08-01'
	group by
		ib_message_thread.thread_id,
		ib_message_thread.message_id
),

msgs as (
	select
		ib_message_thread.thread_id,
		ib_message_thread.message_id
	from
        {{source('clarity_ods', 'ib_message_thread')}} as ib_message_thread
		left join
			{{source('clarity_ods', 'ib_message_threa_1')}} as ib_message_threa_1
				on ib_message_thread.thread_id = ib_message_threa_1.thread_id
				and ib_message_thread.line = ib_message_threa_1.group_line
		left join
			{{source('clarity_ods', 'ib_message_threa_2')}} as ib_message_threa_2
			on ib_message_thread.thread_id = ib_message_threa_2.thread_id
			and ib_message_thread.line = ib_message_threa_2.group_line
	where
		exists (select
					thread_id
				from
					{{source('clarity_ods', 'ib_threads')}} as ib_threads
				where
					ib_threads.thread_id = ib_message_thread.thread_id
				)
		and ib_message_thread.instant_sent_tm >= '2022-08-01'
),

chain_length as (
	select
		threads.thread_id,
		sum(threads.recip_max) as inbasket_chain_length
	from
		threads
	group by
		threads.thread_id
)

select
    {{
        dbt_utils.surrogate_key([
            'msgs.message_id',
            'threads.thread_id'
        ])
    }} as ib_myc_msg_thread_key,
	threads.thread_id as inbasket_thread,
	msgs.message_id as inbasket_msg_id,
	chain_length.inbasket_chain_length
from
	msgs as msgs
	inner join threads
		on threads.thread_id = msgs.thread_id
	inner join chain_length
		on threads.thread_id = chain_length.thread_id
where
	exists (select
				inbasket_msg_id
			from
				{{ ref ('stg_myc_message') }} as stg_myc_message
			where
				stg_myc_message.inbasket_msg_id = msgs.message_id
			)
group by
	threads.thread_id,
	msgs.message_id,
	chain_length.inbasket_chain_length
