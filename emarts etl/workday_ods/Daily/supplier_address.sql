with sup_add as (
    select distinct
        supplier_wid,
        supplier_id,
        supplier_reference_id,
        address_id,
        cf_lrv_address_wid as address_wid,
        substring(address_effective_date, 1, 10) as address_effective_date,
        address_line_1,
        address_line_2,
        address_line_3,
        address_line_4,
        city,
        country_iso_code,
        state_iso_code,
        postal_code,
        formatted_address,
        coalesce(cast(is_primary as int), 0) as is_primary_ind
    from
        {{source('workday_ods', 'workday_supplier_address')}} as workday_supplier_address
)
select
    supplier_wid,
    supplier_id,
    supplier_reference_id,
    address_id,
    address_wid,
    address_effective_date,
    address_line_1,
    address_line_2,
    address_line_3,
    address_line_4,
    city,
    country_iso_code,
    state_iso_code,
    postal_code,
    formatted_address,
    is_primary_ind,
    cast({{
        dbt_utils.surrogate_key([
            'supplier_wid',
            'supplier_id',
            'supplier_reference_id',
            'address_id',
            'address_wid',
            'address_effective_date',
            'address_line_1',
            'address_line_2',
            'address_line_3',
            'address_line_4',
            'city',
            'country_iso_code',
            'state_iso_code',
            'postal_code',
            'formatted_address',
            'is_primary_ind'
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
