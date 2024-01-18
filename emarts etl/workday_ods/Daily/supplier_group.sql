{{
    config(
        materialized = 'incremental',
        unique_key = 'supplier_group_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['supplier_group_wid', 'supplier_group_id', 'supplier_group_name', 'md5', 'upd_dt', 'upd_by']
    )
}}
with sup_group as (
    select distinct
        supplier_group_reference_wid as supplier_group_wid,
        supplier_group_reference_supplier_group_id as supplier_group_id,
        supplier_group_data_supplier_group_name as supplier_group_name,
        cast({{
            dbt_utils.surrogate_key([
                'supplier_group_wid',
                'supplier_group_id',
                'supplier_group_name'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_supplier_groups')}} as get_supplier_groups
)
select
    supplier_group_wid,
    supplier_group_id,
    supplier_group_name,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    sup_group
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                supplier_group_wid = sup_group.supplier_group_wid
        )
    {%- endif %}
