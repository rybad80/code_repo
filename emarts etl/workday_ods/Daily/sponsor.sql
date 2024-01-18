{{ config(
    materialized = 'incremental',
    unique_key = 'sponsor_wid',
    incremental_strategy = 'merge',
    merge_update_columns = [
        'sponsor_wid',
        'sponsor_id',
        'sponsor_reference_id',
        'sponsor_name',
        'sponsor_type_id',
        'md5', 'upd_dt', 'upd_by'
    ]
) }}
select distinct
    sponsor_sponsor_reference_wid as sponsor_wid,
    sponsor_sponsor_reference_sponsor_id as sponsor_id,
    sponsor_sponsor_reference_sponsor_reference_id as sponsor_reference_id,
    sponsor_sponsor_data_sponsor_name as sponsor_name,
    sponsor_sponsor_data_sponsor_type_reference_sponsor_type_id as sponsor_type_id,
    cast({{
        dbt_utils.surrogate_key([
            'sponsor_wid',
            'sponsor_id',
            'sponsor_reference_id',
            'sponsor_name',
            'sponsor_type_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{ source('workday_ods', 'get_sponsors') }} as get_sponsors
where
    1=1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                sponsor_wid = get_sponsors.sponsor_sponsor_reference_wid
        )
    {%- endif %}

