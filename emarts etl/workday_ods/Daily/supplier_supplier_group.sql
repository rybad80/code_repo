{{
    config(
        materialized = 'incremental',
        unique_key = ['supplier_group_wid', 'supplier_wid'],
        incremental_strategy = 'merge',
        merge_update_columns = ['supplier_group_wid', 'supplier_group_id', 'supplier_wid', 'supplier_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
with sup_sup_group as (
    select distinct
        coalesce(supplier_supplier_data_supplier_group_reference_wid, 'N/A') as supplier_group_wid,
        coalesce(supplier_supplier_data_supplier_group_reference_supplier_group_id, 'N/A') as supplier_group_id,
        coalesce(supplier_supplier_reference_wid, 'N/A') as supplier_wid,
        coalesce(supplier_supplier_reference_supplier_id, 'N/A') as supplier_id,
        cast({{
            dbt_utils.surrogate_key([
                'supplier_group_wid',
                'supplier_group_id',
                'supplier_wid',
                'supplier_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_suppliers')}} as get_suppliers
)
select
    supplier_group_wid,
    supplier_group_id,
    supplier_wid,
    supplier_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    sup_sup_group
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                supplier_group_wid = sup_sup_group.supplier_group_wid
                and supplier_wid = sup_sup_group.supplier_wid
        )
    {%- endif %}
