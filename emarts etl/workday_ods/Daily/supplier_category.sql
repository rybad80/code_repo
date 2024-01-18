{{
    config(
        materialized = 'incremental',
        unique_key = 'supplier_category_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['supplier_category_wid', 'supplier_category_id', 'supplier_category_name', 'md5', 'upd_dt', 'upd_by']
    )
}}
with sup_cat as (
    select distinct
        supplier_category_reference_wid as supplier_category_wid,
        supplier_category_reference_supplier_category_id as supplier_category_id,
        supplier_category_data_supplier_category_name as supplier_category_name,
        cast({{
            dbt_utils.surrogate_key([
                'supplier_category_wid',
                'supplier_category_id',
                'supplier_category_name'
                ])
            }} as varchar(100)) as md5,
            current_timestamp as create_dt,
            'WORKDAY' as create_by,
            current_timestamp as upd_dt,
            'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_supplier_categories')}} as get_supplier_categories
)
select
    supplier_category_wid,
    supplier_category_id,
    supplier_category_name,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    sup_cat
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                supplier_category_wid = sup_cat.supplier_category_wid
        )
    {%- endif %}