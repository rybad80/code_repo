{{ config(
    materialized = 'incremental',
    unique_key = 'manufacturer_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['manufacturer_wid','manufacturer_id','manufacturer_name','manufacturer_url_reference','inactive_ind', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    wid as manufacturer_wid,
    manufacturer_id as manufacturer_id,
    manufacturer_name_reference as manufacturer_name,
    manufacturer_url_reference,
    coalesce(cast(inactive as int),-2) as inactive_ind,
    cast({{ 
        dbt_utils.surrogate_key([ 
            'manufacturer_wid',
            'manufacturer_id',
            'manufacturer_name',
            'manufacturer_url_reference',
            'inactive_ind'
        ]) 
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods','get_manufacturers')}} as get_manufacturers
where
1 = 1
{%- if is_incremental() %}
    and md5 not in (
        select md5
        from
            {{ this }}
        where
            manufacturer_wid = get_manufacturers.wid
        ) 
{%- endif %}