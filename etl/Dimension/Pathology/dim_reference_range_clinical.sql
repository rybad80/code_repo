{{
  config(
    meta = {
      'critical': false
    }
  )
}}
-- Specific hematology reference ranges structured in a different way in Clarity
with hem_anc_lpp as (
select clarity_lpp.lpp_id,
clarity_lpp.lpp_name,
upper(regexp_extract(clarity_lpp.lpp_name, '[^ ]+', 1, 5)) as sex,
cast(regexp_extract(clarity_lpp.lpp_name, '[0-9]+', 1, 1) as int) as age_low_val,
case
    when upper(trim(regexp_extract(clarity_lpp.lpp_name, '[^0-9-]+', 1, 2))) != ''
        then upper(trim(regexp_extract(clarity_lpp.lpp_name, '[^0-9-]+', 1, 2)))
    else upper(trim(regexp_extract(clarity_lpp.lpp_name, '[^0-9]+', 1, 3)))
end as age_low_units,
cast(regexp_extract(clarity_lpp.lpp_name, '[0-9]+', 1, 2) as int) as age_high_val,
upper(trim(regexp_extract(clarity_lpp.lpp_name, '[^0-9]+', 1, 3))) as age_high_units
from {{source('clarity_ods', 'clarity_lpp')}} as clarity_lpp
where clarity_lpp.lpp_name like 'HEM LAB RC ANC%'
),
-- Programming point for unspecified sex ranges
unspecified_sex_lpp as (
select clarity_lpp.lpp_id,
clarity_lpp.lpp_name
from {{source('clarity_ods', 'clarity_lpp')}} as clarity_lpp
where clarity_lpp.lpp_name = 'CHOP LAB UNSPECIFIED SEX RESULT CHECKING LPP'
),
-- Programming points to exclude
lpp_exclusion as (
select clarity_lpp.lpp_id,
clarity_lpp.lpp_name
from {{source('clarity_ods', 'clarity_lpp')}} as clarity_lpp
where clarity_lpp.lpp_name in (
    'BKR Metabolic nM less than 0',
    'Lab Order is not QC'
    )
),
-- Point of care programming points
poc_source_lpp as (
select clarity_lpp.lpp_id,
clarity_lpp.lpp_name
from {{source('clarity_ods', 'clarity_lpp')}} as clarity_lpp
where lower(clarity_lpp.lpp_name) like '%poc%result checking%'
),
tre_rc_def as (
select tre_rc_def_snapshot.record_id,
tre_rc_def_snapshot.contact_date_real,
tre_rc_def_snapshot.line,
tre_rc_def_snapshot.rc_def_link,
tre_rc_def_snapshot.rc_value_low,
tre_rc_def_snapshot.rc_value_high,
tre_rc_def_snapshot.rc_inc_exc_endpts_c,
tre_rc_def_snapshot.dbt_updated_at,
tre_rc_def_snapshot.dbt_valid_from,
tre_rc_def_snapshot.dbt_valid_to
from {{ref('tre_rc_def_snapshot')}} as tre_rc_def_snapshot
where tre_rc_def_snapshot.rc_check_type = 2.2 -- abnormal result
),
-- Get all the reference ranges
refrange_components as (
select clarity_component.component_id,
tre_rc_param.record_id,
tre_rc_param.rc_linked_def,
tre_rc_param.contact_date_real,
tre_rc_param.line,
clarity_component.common_name,
clarity_component.name as component_name,
zc_spec_source.name as specimen_source,
method_db_main.method_name,
method_db_main.method_id,
case
    when unspecified_sex_lpp.lpp_id is not null then 'UNSPECIFIED'
    else zc_sex.abbr
end as sex,
cast(tre_rc_param.rc_age as int) as rc_age,
clarity_component.dflt_units as rc_units,
zc_rc_age_units.title
from {{source('clarity_ods', 'clarity_component')}} as clarity_component
inner join {{source('clarity_ods', 'tre_rc_param')}} as tre_rc_param
    on clarity_component.result_checking_id = tre_rc_param.record_id
left join {{source('clarity_ods', 'zc_sex')}} as zc_sex
    on zc_sex.internal_id = tre_rc_param.rc_sex_c
left join {{source('clarity_ods', 'zc_rc_age_units')}} as zc_rc_age_units
    on zc_rc_age_units.rc_age_units_c =  tre_rc_param.rc_age_units_c
left join {{source('clarity_ods', 'zc_spec_source')}} as zc_spec_source
	on zc_spec_source.spec_source_c = tre_rc_param.rc_spec_source_c
left join {{source('clarity_ods', 'method_db_main')}} as method_db_main
    on method_db_main.method_id = tre_rc_param.rc_method_id
inner join {{ref('stg_reported_components')}} as stg_reported_components
    on stg_reported_components.component_id = clarity_component.component_id
left join unspecified_sex_lpp
    on tre_rc_param.rc_ppt_id = unspecified_sex_lpp.lpp_id
left join hem_anc_lpp
    on tre_rc_param.rc_ppt_id = hem_anc_lpp.lpp_id
left join lpp_exclusion
    on tre_rc_param.rc_ppt_id = lpp_exclusion.lpp_id
left join poc_source_lpp
    on tre_rc_param.rc_ppt_id = poc_source_lpp.lpp_id
where clarity_component.component_id >= 123020000 -- filters CP only
    and hem_anc_lpp.lpp_id is null
    and lpp_exclusion.lpp_id is null
    and poc_source_lpp.lpp_id is null
    and stg_reported_components.reported_ind = 1
),
-- Lag the ranges to get the age range for each line
lag_ranges as (
select refrange_components.component_id,
refrange_components.record_id,
refrange_components.rc_linked_def,
refrange_components.contact_date_real,
refrange_components.line,
refrange_components.common_name,
refrange_components.component_name,
refrange_components.specimen_source,
refrange_components.method_name,
refrange_components.method_id,
refrange_components.sex,
refrange_components.rc_age,
refrange_components.rc_units,
refrange_components.title,
coalesce(lag(refrange_components.rc_age, 1)
    over(partition by
        refrange_components.record_id,
        refrange_components.contact_date_real,
        refrange_components.method_name,
        refrange_components.specimen_source,
        refrange_components.sex
        order by
            refrange_components.contact_date_real,
            refrange_components.method_name,
            refrange_components.specimen_source,
            refrange_components.sex,
            case
                when refrange_components.title = 'DAYS' then refrange_components.rc_age
                when refrange_components.title = 'WEEKS' then refrange_components.rc_age * 7
                when refrange_components.title = 'MONTHS' then refrange_components.rc_age * 30
                when refrange_components.title = 'YEARS' then refrange_components.rc_age * 365
            end),
        case when refrange_components.rc_age is not null then 0 else null end) as age_low_val,
coalesce(lag(refrange_components.title, 1)
    over(partition by
        refrange_components.record_id,
        refrange_components.contact_date_real,
        refrange_components.method_name,
        refrange_components.specimen_source,
        refrange_components.sex
        order by
            refrange_components.contact_date_real,
            refrange_components.method_name,
            refrange_components.specimen_source,
            refrange_components.sex,
            case
                when refrange_components.title = 'DAYS' then refrange_components.rc_age
                when refrange_components.title = 'WEEKS' then refrange_components.rc_age * 7
                when refrange_components.title = 'MONTHS' then refrange_components.rc_age * 30
                when refrange_components.title = 'YEARS' then refrange_components.rc_age * 365
            end),
        refrange_components.title) as age_low_units,
refrange_components.rc_age as age_high_val,
refrange_components.title as age_high_units
from refrange_components
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
lag_ranges.component_id,
lag_ranges.component_name,
lag_ranges.common_name,
coalesce(lag_ranges.specimen_source, 'DEFAULT') as specimen_source,
coalesce(lag_ranges.method_name, 'DEFAULT') as method_name,
coalesce(lag_ranges.method_id, '0') as method_id,
coalesce(lag_ranges.sex, 'DEFAULT') as sex,
coalesce(cast(lag_ranges.age_low_val as nvarchar(12)), 'DEFAULT') as age_low_val,
coalesce(lag_ranges.age_low_units, 'DEFAULT') as age_low_units,
coalesce(cast(lag_ranges.age_high_val as nvarchar(12)), 'DEFAULT') as age_high_val,
coalesce(lag_ranges.age_high_units, 'DEFAULT') as age_high_units,
coalesce(lag_ranges.age_low_val || ' '
    || lag_ranges.age_low_units || ' - '
    || lag_ranges.age_high_val || ' '
    || lag_ranges.age_high_units , 'DEFAULT') as age_range,
coalesce(tre_rc_def.rc_value_low, 'DEFAULT') as value_low,
coalesce(tre_rc_def.rc_value_high, 'DEFAULT') as value_high,
coalesce(lag_ranges.rc_units, 'DEFAULT') as value_units,
-- Convoluted way to display the reference range as text,
-- Taking into account endpoint behavior
case
    when zc_rc_inc_exc_endp.name is null
        or zc_rc_inc_exc_endp.name = 'None' then 'DEFAULT'
    else zc_rc_inc_exc_endp.name
end as value_endpoint_behavior,
(case when lower(zc_rc_inc_exc_endp.name) in ('reverse lower bound', 'reverse both') then '> ' else '' end)
    || coalesce(tre_rc_def.rc_value_low, '')
    || (case when tre_rc_def.rc_value_low is not null
        and tre_rc_def.rc_value_high is not null then ' to ' else '' end)
    || (case when lower(zc_rc_inc_exc_endp.name) in ('reverse upper bound', 'reverse both') then '< ' else '' end)
    || coalesce(tre_rc_def.rc_value_high, '')
    || (case when lag_ranges.rc_units is null
        or (tre_rc_def.rc_value_low is null and tre_rc_def.rc_value_high is null) then ''
        else ' ' || lag_ranges.rc_units end) as value_range,
tre_rc_def.dbt_updated_at,
tre_rc_def.dbt_valid_from,
tre_rc_def.dbt_valid_to,
case when tre_rc_def.dbt_valid_to is null then 1 else 0 end as current_ind
from lag_ranges
inner join tre_rc_def
    on tre_rc_def.rc_def_link = lag_ranges.rc_linked_def and tre_rc_def.record_id = lag_ranges.record_id
left join {{source('clarity_ods','zc_rc_inc_exc_endp')}} as zc_rc_inc_exc_endp
    on zc_rc_inc_exc_endp.rc_inc_exc_endp_c = tre_rc_def.rc_inc_exc_endpts_c
where not(
    tre_rc_def.rc_value_low is null
    and tre_rc_def.rc_value_high is null
    and lag_ranges.rc_age is null
)

union all

-- Hematology reference ranges with unique structure
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
coalesce(zc_spec_source.name, 'DEFAULT') as specimen_source,
coalesce(method_db_main.method_name, 'DEFAULT') as method_name,
coalesce(method_db_main.method_id, '0') as method_id,
coalesce(hem_anc_lpp.sex, 'DEFAULT') as sex,
coalesce(cast(hem_anc_lpp.age_low_val as nvarchar(12)), 'DEFAULT') as age_low_val,
coalesce(hem_anc_lpp.age_low_units, 'DEFAULT') as age_low_units,
coalesce(cast(hem_anc_lpp.age_high_val as nvarchar(12)), 'DEFAULT') as age_high_val,
coalesce(hem_anc_lpp.age_high_units, 'DEFAULT') as age_high_units,
coalesce(hem_anc_lpp.age_low_val || ' '
    || hem_anc_lpp.age_low_units || ' - '
    || hem_anc_lpp.age_high_val || ' '
    || hem_anc_lpp.age_high_units, 'DEFAULT') as age_range,
coalesce(tre_rc_def.rc_value_low, 'DEFAULT') as value_low,
coalesce(tre_rc_def.rc_value_high, 'DEFAULT') as value_high,
coalesce(clarity_component.dflt_units, 'DEFAULT') as value_units,
-- Convoluted way to display the reference range as text,
-- Taking into account endpoint behavior
case
    when zc_rc_inc_exc_endp.name is null
        or zc_rc_inc_exc_endp.name = 'None' then 'DEFAULT'
    else zc_rc_inc_exc_endp.name
end as value_endpoint_behavior,
(case when lower(zc_rc_inc_exc_endp.name) in ('reverse lower bound', 'reverse both') then '> ' else '' end)
    || coalesce(tre_rc_def.rc_value_low, '')
    || (case when tre_rc_def.rc_value_low is not null
        and tre_rc_def.rc_value_high is not null then ' to ' else '' end)
    || (case when lower(zc_rc_inc_exc_endp.name) in ('reverse upper bound', 'reverse both') then '< ' else '' end)
    || coalesce(tre_rc_def.rc_value_high, '')
    || (case when clarity_component.dflt_units is null
        or (tre_rc_def.rc_value_low is null and tre_rc_def.rc_value_high is null) then ''
        else ' ' || clarity_component.dflt_units end) as value_range,
tre_rc_def.dbt_updated_at,
tre_rc_def.dbt_valid_from,
tre_rc_def.dbt_valid_to,
case when tre_rc_def.dbt_valid_to is null then 1 else 0 end as current_ind
from {{source('clarity_ods', 'clarity_component')}} as clarity_component
inner join {{source('clarity_ods', 'tre_rc_param')}} as tre_rc_param
    on clarity_component.result_checking_id = tre_rc_param.record_id
inner join {{ref('tre_rc_def_snapshot')}} as tre_rc_def
    on tre_rc_def.rc_def_link = tre_rc_param.rc_linked_def and tre_rc_param.record_id = tre_rc_def.record_id
left join {{source('clarity_ods','zc_rc_inc_exc_endp')}} as zc_rc_inc_exc_endp
    on zc_rc_inc_exc_endp.rc_inc_exc_endp_c = tre_rc_def.rc_inc_exc_endpts_c
left join {{source('clarity_ods', 'zc_spec_source')}} as zc_spec_source
	on zc_spec_source.spec_source_c = tre_rc_param.rc_spec_source_c
left join {{source('clarity_ods', 'method_db_main')}} as method_db_main
    on method_db_main.method_id = tre_rc_param.rc_method_id
inner join {{ref('stg_reported_components')}} as stg_reported_components
    on stg_reported_components.component_id = clarity_component.component_id
inner join hem_anc_lpp
    on tre_rc_param.rc_ppt_id = hem_anc_lpp.lpp_id
where tre_rc_def.rc_check_type = 2.2 -- abnormal result
    and clarity_component.component_id >= 123020000 -- filters CP only
    and stg_reported_components.reported_ind = 1
