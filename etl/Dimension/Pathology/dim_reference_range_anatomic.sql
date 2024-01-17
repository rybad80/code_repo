{{
  config(
    meta = {
      'critical': false
    }
  )
}}
-- Programming points to get the demographic breakdown
with lpp as (
select clarity_lpp.lpp_id,
case
    when clarity_lpp.lpp_name like '% F' then 'F'
    when clarity_lpp.lpp_name like '% M' then 'M'
    -- Non-gestational are male if no gender is specified
    when clarity_lpp.lpp_name not like '%Gestational%'
        and clarity_lpp.lpp_name not like '% F' then 'M'
    else null
end as sex,
case
    when clarity_lpp.lpp_name like 'Lab RC Age %'
        then regexp_extract(clarity_lpp.lpp_name, '[^ ]+', 1, 4)
    when clarity_lpp.lpp_name like 'Lab RC Gestational Age %'
        then regexp_extract(clarity_lpp.lpp_name, '[^ ]+', 1, 5)
    else null
end as age,
case
    when clarity_lpp.lpp_name like 'Lab RC Age %'
        then upper(regexp_extract(clarity_lpp.lpp_name, '[^ ]+', 1, 5))
    when clarity_lpp.lpp_name like 'Lab RC Gestational Age %'
        then upper(regexp_extract(clarity_lpp.lpp_name, '[^ ]+', 1, 6))
    else null
end as age_units,
case
    when clarity_lpp.lpp_name like '%Gestational%' then 1
    else 0
end as age_gestational_ind
from {{source('clarity_ods', 'clarity_lpp')}} as clarity_lpp
)
select
{{ dbt_utils.surrogate_key([
    'tre_rc_def.record_id',
    'tre_rc_def.contact_date_real',
    'tre_rc_def.line',
    'tre_rc_def.dbt_valid_from'
]) }} as reference_range_key,
tre_rc_def.record_id as tre_id,
tre_rc_def.contact_date_real,
tre_rc_def.line,
clarity_component.component_id,
clarity_component.name as component_name,
clarity_component.common_name,
coalesce(lpp.sex, 'DEFAULT') as sex,
lpp.age,
lpp.age_units,
lpp.age_gestational_ind,
lpp.age || ' ' || lpp.age_units
|| case
    when lpp.age_gestational_ind = 1 then ' GESTATIONAL'
    else ''
end as age_range,
tre_rc_def.rc_value_low as value_low,
tre_rc_def.rc_value_high as value_high,
clarity_component.dflt_units as value_units,
tre_rc_def.rc_value_low || ' - ' || tre_rc_def.rc_value_high || ' ' || clarity_component.dflt_units as value_range,
tre_rc_def.dbt_updated_at,
tre_rc_def.dbt_valid_from,
tre_rc_def.dbt_valid_to,
case when tre_rc_def.dbt_valid_to is null then 1 else 0 end as current_ind
from {{source('clarity_ods', 'clarity_component')}} as clarity_component
inner join {{source('clarity_ods', 'tre_rc_param')}} as tre_rc_param
    on clarity_component.result_checking_id = tre_rc_param.record_id
inner join {{ref('tre_rc_def_snapshot')}} as tre_rc_def
    on tre_rc_def.rc_def_link = tre_rc_param.rc_linked_def and tre_rc_param.record_id = tre_rc_def.record_id
left join {{source('clarity_ods', 'zc_spec_source')}} as zc_spec_source
	on zc_spec_source.spec_source_c = tre_rc_param.rc_spec_source_c
left join lpp
	on tre_rc_param.rc_ppt_id = lpp.lpp_id
where tre_rc_def.rc_check_type = 2.2 -- abnormal result
    and clarity_component.component_id between 123010000 and 123020000 -- filters AP only
