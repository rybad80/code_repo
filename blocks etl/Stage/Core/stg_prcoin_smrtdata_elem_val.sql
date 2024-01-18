{{ config(meta = {
    'critical': true
}) }}

with evp_count as (
	select
		hlv_id,
		max(line) as prev_val_line
	from
		{{source('clarity_ods','elem_val_prev')}}
	group by
		hlv_id
),
evp as (
	select
		hlv_id,
		line,
		prev_instant_dttm,
		cast(case
				when prev_val_pointer = 'null' then null
				else prev_val_pointer
			end as int)
		-- needed because the string 'null' can be stored to prev_val_pointer
		as prev_val_pointer
		from
		{{source('clarity_ods','elem_val_prev')}}
),
evpv_line as (
	select
		hlv_id,
		line,
		cast(case
				when ltrim(prev_values, '0123456789') is null then prev_values
				else null
			end as int) as event_lines -- this is a varchar column, but we only want the integer pointers
	from
		{{source('clarity_ods','elem_val_prev_val')}}
)
--first, compile all historical values for each sde
(
	select
		sld.hlv_id,
		evp.line as update_num, -- the number of contacts for the sde, higher = more recent
		case
			when (evpv_value.line - evpv_line.line) is null then 1
			else (evpv_value.line - evpv_line.line)
		end as line, -- hlv_id + event_num + line is the pk
		sld.cm_phy_owner_id as cm_phy_owner_id,
		sld.cm_log_owner_id as cm_log_owner_id,
		sld.element_id,
		evpv_value.prev_values as smrtdta_elem_value,
		'N' as latest_value_yn, -- indicates this is a historical value
		evp.prev_instant_dttm as value_update_dttm,
		sld.context_name as context_name,
		sld.contact_serial_num as contact_serial_num,
		sld.record_id_varchar as record_id_varchar,
		sld.record_id_numeric as record_id_numeric,
		sld.update_date as update_date,
		sld.pat_link_id as pat_link_id
	from
		{{source('clarity_ods','smrtdta_elem_data')}} as sld
		inner join evp on sld.hlv_id = evp.hlv_id
		left outer join evpv_line on evp.hlv_id = evpv_line.hlv_id and evp.prev_val_pointer = evpv_line.line
		left outer join {{source('clarity_ods','elem_val_prev_val')}} as evpv_value
			on evpv_line.hlv_id = evpv_value.hlv_id
			and evpv_line.line + evpv_line.event_lines >= evpv_value.line
			and evpv_line.line < evpv_value.line
)
union all --rows will be distinct between queries, so union all is fine and avoids a sort
--then, include the current value for each sde
(
	select
		sld.hlv_id,
		--this makes UPDATE_NUM one more than the biggest historical value, or '1' if there are no historical values
		coalesce(evp_count.prev_val_line, 0) + 1 as update_num,
		coalesce(slv.line, 1) as line,
		sld.cm_phy_owner_id as cm_phy_owner_id,
		sld.cm_log_owner_id as cm_log_owner_id,
		sld.element_id,
		slv.smrtdta_elem_value as smrtdta_elem_value,
		'Y' as latest_value_yn, --indicates this is a current value
		sld.cur_value_datetime as value_update_dttm,
		sld.context_name as context_name,
		sld.contact_serial_num as contact_serial_num,
		sld.record_id_varchar as record_id_varchar,
		sld.record_id_numeric as record_id_numeric,
		sld.update_date as update_date,
		sld.pat_link_id as pat_link_id
	from
		{{source('clarity_ods','smrtdta_elem_data')}} as sld
		left outer join evp_count
			on sld.hlv_id = evp_count.hlv_id
		left outer join {{source('clarity_ods','smrtdta_elem_value')}} as slv
			on sld.hlv_id = slv.hlv_id
)
