{{
  config(
    meta = {
      'critical': false
    }
  )
}}
select
{{ dbt_utils.surrogate_key([
    'tre_rc_def_cat.record_id',
    'tre_rc_def_cat.contact_date_real',
    'tre_rc_def_cat.group_line',
    'tre_rc_def_cat.value_line',
    'tre_rc_def_cat.dbt_valid_from'
]) }} as reference_range_key,
tre_rc_def_cat.record_id as tre_id,
tre_rc_def_cat.contact_date_real,
tre_rc_def_cat.group_line,
tre_rc_def_cat.value_line,
tre_rc_def.rc_check_type as result_category,
tre_rc_def.rc_check_type_name as result_category_name,
clarity_component.component_id,
clarity_component.name as component_name,
clarity_component.common_name,
coalesce(zc_spec_source.name, 'DEFAULT') as specimen_source,
coalesce(method_db_main.method_name, 'DEFAULT') as method_name,
coalesce(method_db_main.method_id, '0') as method_id,
coalesce(tre_rc_def_cat.rc_value_cat_title, 'DEFAULT') as value,
coalesce(clarity_component.dflt_units, 'DEFAULT') as value_units,
tre_rc_def_cat.dbt_updated_at,
tre_rc_def_cat.dbt_valid_from,
tre_rc_def_cat.dbt_valid_to,
case when tre_rc_def_cat.dbt_valid_to is null then 1 else 0 end as current_ind
from {{source('clarity_ods', 'clarity_component')}} as clarity_component
inner join {{source('clarity_ods', 'tre_rc_param')}} as tre_rc_param
    on clarity_component.result_checking_id = tre_rc_param.record_id
left join {{source('clarity_ods', 'zc_spec_source')}} as zc_spec_source
	on zc_spec_source.spec_source_c = tre_rc_param.rc_spec_source_c
inner join {{ref('tre_rc_def_snapshot')}} as tre_rc_def
    on tre_rc_def.rc_def_link = tre_rc_param.rc_linked_def and tre_rc_param.record_id = tre_rc_def.record_id
left join {{source('clarity_ods', 'method_db_main')}} as method_db_main
    on method_db_main.method_id = tre_rc_param.rc_method_id
inner join {{ref('tre_rc_def_cat_snapshot')}} as tre_rc_def_cat
    on tre_rc_def_cat.record_id = tre_rc_def.record_id
        and tre_rc_def_cat.group_line = tre_rc_def.line
inner join {{ref('stg_reported_components')}} as stg_reported_components
    on stg_reported_components.component_id = clarity_component.component_id
where clarity_component.component_id >= 123020000 -- filters CP only
    and tre_rc_def.rc_check_type in (1.1, 2.2) -- abnormal result
    -- filter out Female URINE SPERM records
    and not (clarity_component.component_id = 123050161 and tre_rc_param.rc_sex_c is not null)
    and stg_reported_components.reported_ind = 1
