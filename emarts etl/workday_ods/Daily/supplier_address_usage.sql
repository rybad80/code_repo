with sup_add as (
    select distinct
        supplier_reference_id,
        supplier_wid,
        supplier_id,
        address_id,
        cf_lrv_address_wid as address_wid,
        usage_type as address_usage_reference_type,
        address_usage as address_usage_id
    from
        {{source('workday_ods', 'workday_supplier_address')}} as workday_supplier_address
)
select
    supplier_wid,
    supplier_id,
    supplier_reference_id,
    address_id,
    address_wid,
    address_usage_id,
    cast({{
        dbt_utils.surrogate_key([
            'supplier_reference_id',
            'supplier_wid',
            'supplier_id',
            'address_id',
            'address_wid',
            'address_usage_reference_type',
            'address_usage_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    sup_add
where
    1 = 1
