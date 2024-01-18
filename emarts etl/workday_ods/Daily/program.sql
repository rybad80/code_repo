{{ config(
    materialized = 'incremental',
    unique_key = 'program_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['program_wid','program_id','program_name','include_program_id_in_name_ind', 'program_is_inactive_ind', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    program_reference_wid as program_wid,
    program_reference_program_id as program_id,
    program_data_program_name as program_name,
    program_data_include_program_id_in_name as include_program_id_in_name_ind,
    program_data_program_is_inactive as program_is_inactive_ind,
    cast({{
        dbt_utils.surrogate_key([
            'program_wid',
            'program_id',
            'program_name',
            'include_program_id_in_name_ind',
            'program_is_inactive_ind'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_programs_data')}} as get_programs_data
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                program_wid = get_programs_data.program_reference_wid
        )
    {%- endif %}
