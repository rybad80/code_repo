{{
    config(
        materialized='incremental',
        unique_key = 'smart_data_history_key',
	meta = {'critical': false}    
    )
}}


with base as (
    select
    hlv_id,
    line,
	case when prev_values = '' then null else regexp_replace(prev_values, '^\d+$', '') end as clean_prev,
	cast(case when clean_prev = '' and length(prev_values) <= 9
        then prev_values else null
    end as bigint) as event_lines
	from
    {{source('clarity_ods', 'elem_val_prev_val')}}
	)


select
    smrtdta_elem_data.hlv_id as sde_id,
    smart_data_element_info.sde_key,
	smrtdta_elem_data.element_id,
	smrtdta_elem_data.context_name,
	smrtdta_elem_data.pat_link_id,
	elem_val_prev.line as previous_version_num,
	case when (elem_val_prev_val.line - base.line) is null
        then 1 else (elem_val_prev_val.line - base.line)
	end as seq_num,
    elem_val_prev_val.prev_values as sde_previous_value,
	elem_val_prev.prev_instant_dttm as previous_value_update_date,
    {{dbt_utils.surrogate_key(
            ['smrtdta_elem_data.hlv_id',
            'elem_val_prev.line', 
            'case when (elem_val_prev_val.line - base.line) is null
        then 1 else (elem_val_prev_val.line - base.line) end'
            ])
    }} as smart_data_history_key
from  {{source('clarity_ods', 'smrtdta_elem_data')}} as smrtdta_elem_data
inner join  {{source('clarity_ods', 'elem_val_prev')}}  as elem_val_prev
	on smrtdta_elem_data.hlv_id = elem_val_prev.hlv_id
left join   {{source('cdw', 'smart_data_element_info')}} as smart_data_element_info
    on elem_val_prev.hlv_id = smart_data_element_info.sde_id
left join  base as base
	on elem_val_prev.hlv_id = base.hlv_id
	and cast(case when elem_val_prev.prev_val_pointer = 'null'
                then null else elem_val_prev.prev_val_pointer end as int
        ) = base.line
left join  {{source('clarity_ods', 'elem_val_prev_val')}} as elem_val_prev_val
	on base.hlv_id = elem_val_prev_val.hlv_id
	and  base.line + base.event_lines  >= elem_val_prev_val.line
	and base.line < elem_val_prev_val.line
where smrtdta_elem_data.cur_value_datetime >= '2018-01-01'
{% if is_incremental() %}
    and date(previous_value_update_date) >= (select max(date(previous_value_update_date)) from {{ this }})
{% endif %}
