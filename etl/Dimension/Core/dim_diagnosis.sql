{% set column_names = ['diagnosis_id', 'ref_bill_code_set_c', 'record_type_c',
        'diagnosis_name', 'record_state_c', 'diagnosis_group', 'ref_bill_code', 'parent_diagnosis_id',
		'external_diagnosis_id', 'diagnosis_other_desc', 'current_icd9_code', 'current_icd10_code',
		'current_icd9_list', 'current_icd10_list', 'ec_inactive_ind', 'spec_billing_ind', 'shown_in_myc_ind'] %}

{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = column_names,
      meta = {
      'critical': true
    }
  )
}}

with unionset as (
select
	{{
        dbt_utils.surrogate_key([
            'clarity_edg.dx_id',
			'\'CLARITY\''
        ])
    }} as diagnosis_key,
	clarity_edg.dx_id as diagnosis_id,
	clarity_edg.ref_bill_code_set_c,
	clarity_edg.record_type_c,
	clarity_edg.dx_name as diagnosis_name,
	clarity_edg.record_state_c,
	clarity_edg.dx_group as diagnosis_group,
	clarity_edg.ref_bill_code,
	clarity_edg.parent_dx_id as parent_diagnosis_id,
	clarity_edg.external_id as external_diagnosis_id,
	clarity_edg.dx_other_desc as diagnosis_other_desc,
	edg_current_icd9.code as current_icd9_code,
    edg_current_icd10.code as current_icd10_code,
	clarity_edg.current_icd9_list,
	clarity_edg.current_icd10_list,
	case
		when clarity_edg.ec_inactive_yn = 'Y' then 1
		when clarity_edg.ec_inactive_yn = 'N' then 0
		else -2
	end as ec_inactive_ind,
	case
		when clarity_edg.spec_billing_yn = 'Y' then 1
		when clarity_edg.spec_billing_yn = 'N' then 0
		else -2
	end as spec_billing_ind,
	case
		when clarity_edg.shown_in_myc_yn = 'Y' then 1
		when clarity_edg.shown_in_myc_yn = 'N' then 0
		else -2
	end as shown_in_myc_ind,
	{{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
	'CLARITY' || '~' || clarity_edg.dx_id as integration_id,
	current_timestamp as create_date,
	'CLARITY' as create_source,
	current_timestamp as update_date,
	'CLARITY' as update_source
from
    {{source('clarity_ods','clarity_edg')}} as clarity_edg
    left join {{source('clarity_ods','edg_current_icd9')}} as edg_current_icd9
        on edg_current_icd9.dx_id = clarity_edg.dx_id
        and edg_current_icd9.line = 1
    left join {{source('clarity_ods','edg_current_icd10')}} as edg_current_icd10
        on edg_current_icd10.dx_id = clarity_edg.dx_id
        and edg_current_icd10.line = 1

union all

select
	-1 as diagnosis_key,
	-1 as diagnosis_id,
	-2 as ref_bill_code_set_c,
	0 as record_type_c,
	'INVALID' as diagnosis_name,
	null as record_state_c,
	null as diagnosis_group,
	null as ref_bill_code,
	null as parent_diagnosis_id,
	null as external_diagnosis_id,
	null as diagnosis_other_desc,
	null as current_icd9_code,
    null as current_icd10_code,
	null as current_icd9_list,
	null as current_icd10_list,
	-2 as ec_inactive_ind,
	-2 as spec_billing_ind,
	-2 as shown_in_myc_ind,
	-1 as hash_value,
	'NA' as integration_id,
	current_timestamp as create_date,
	'DEFAULT' as create_source,
	current_timestamp as update_date,
	'DEFAULT' as update_source

union all

select
	0 as diagnosis_key,
	0 as diagnosis_id,
	-2 as ref_bill_code_set_c,
	0 as record_type_c,
	'DEFAULT' as diagnosis_name,
	null as record_state_c,
	null as diagnosis_group,
	null as ref_bill_code,
	null as parent_diagnosis_id,
	null as external_diagnosis_id,
	null as diagnosis_other_desc,
	null as current_icd9_code,
    null as current_icd10_code,
	null as current_icd9_list,
	null as current_icd10_list,
	-2 as ec_inactive_ind,
	-2 as spec_billing_ind,
	-2 as shown_in_myc_ind,
	0 as hash_value,
	'NA' as integration_id,
	current_timestamp as create_date,
	'DEFAULT' as create_source,
	current_timestamp as update_date,
	'DEFAULT' as update_source
)

select
	diagnosis_key,
	diagnosis_id,
	cast(ref_bill_code_set_c as numeric) as ref_bill_code_set_c,
	cast(record_type_c as numeric) as record_type_c,
	cast(diagnosis_name as varchar(200)) as diagnosis_name,
	cast(record_state_c as numeric) as record_state_c,
	cast(diagnosis_group as varchar(200)) as diagnosis_group,
	cast(ref_bill_code as nvarchar(200)) as ref_bill_code,
	parent_diagnosis_id,
	cast(external_diagnosis_id as varchar(254)) as external_diagnosis_id,
	cast(diagnosis_other_desc as varchar(254)) as diagnosis_other_desc,
	cast(current_icd9_code as nvarchar(20)) as current_icd9_code,
    cast(current_icd10_code as nvarchar(20)) as current_icd10_code,
	cast(current_icd9_list as nvarchar(100)) as current_icd9_list,
	cast(current_icd10_list as nvarchar(100)) as current_icd10_list,
	ec_inactive_ind,
	spec_billing_ind,
	shown_in_myc_ind,
	hash_value,
	integration_id,
	create_date,
	create_source,
	update_date,
	update_source
from
	unionset
where
    1 = 1
    {%- if is_incremental() %}
        and hash_value not in
        (
            select
                hash_value
            from
                {{ this }} -- TDL dim table
            where integration_id = unionset.integration_id
        )
    {%- endif %}
