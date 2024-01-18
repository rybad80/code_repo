{{
    config(
        materialized = 'incremental',
        unique_key = 'supplier_item_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['supplier_item_wid','supplier_item_id','item_name','item_identifier','sourcing_priority','calculate_price_based_on_conversion_factor_ind','item_lead_time','unit_price','supplier_wid','supplier_reference_id','supplier_id','supplier_item_supplier_contract_wid','supplier_item_supplier_contract_id','related_purchase_item_wid','related_purchase_item_id','unit_of_measure_descriptor','currency_descriptor', 'md5', 'upd_dt', 'upd_by']
    )
}}
with sup_items as (
    select
        supplier_item_wid,
        supplier_item_reference_id as supplier_item_id,
        item_name,
        item_identifier,
        cast(sourcing_priority as int) as sourcing_priority,
        coalesce(cast(calculate_price_based_on_conversion_factor as int), -2) as calculate_price_based_on_conversion_factor_ind,
        cast(item_lead_time as int) as item_lead_time,
        cast(unit_price as numeric(26,6)) as unit_price,
        supplier_wid,
        supplier_reference_id,
        supplier_id,
        max(supplier_item_supplier_contract_wid) as supplier_item_supplier_contract_wid,
        max(supplier_item_supplier_contract_id) as supplier_item_supplier_contract_id,
        related_purchase_item_wid,
        related_purchase_item_id,
        supplier_item_unit_of_measure as unit_of_measure_descriptor,
        currency as currency_descriptor
    from
        {{source('workday_ods', 'workday_supplier_items')}} as workday_supplier_items
    group by
        supplier_item_wid,
        supplier_item_id,
        item_name,
        item_identifier,
        sourcing_priority,
        calculate_price_based_on_conversion_factor_ind,
        item_lead_time,
        unit_price,
        supplier_wid,
        supplier_reference_id,
        supplier_id,
        related_purchase_item_wid,
        related_purchase_item_id,
        unit_of_measure_descriptor,
        currency_descriptor
)
select
    supplier_item_wid,
    supplier_item_id,
    item_name,
    item_identifier,
    sourcing_priority,
    calculate_price_based_on_conversion_factor_ind,
    item_lead_time,
    unit_price,
    supplier_wid,
    supplier_reference_id,
    supplier_id,
    supplier_item_supplier_contract_wid,
    supplier_item_supplier_contract_id,
    related_purchase_item_wid,
    related_purchase_item_id,
    unit_of_measure_descriptor,
    currency_descriptor,
    cast({{
        dbt_utils.surrogate_key([
            'supplier_item_wid',
            'supplier_item_id',
            'item_name',
            'item_identifier',
            'sourcing_priority',
            'calculate_price_based_on_conversion_factor_ind',
            'item_lead_time',
            'unit_price',
            'supplier_wid',
            'supplier_reference_id',
            'supplier_id',
            'supplier_item_supplier_contract_wid',
            'supplier_item_supplier_contract_id',
            'related_purchase_item_wid',
            'related_purchase_item_id',
            'unit_of_measure_descriptor',
            'currency_descriptor'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    sup_items
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                supplier_item_wid = sup_items.supplier_item_wid
        )
    {%- endif %}
