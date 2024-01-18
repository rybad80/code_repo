with purch_itm_supp as (
    select distinct
        purchase_item_purchase_item_reference_wid as purchase_item_wid,
        purchase_item_purchase_item_reference_purchase_item_id as purchase_item_id,
        coalesce(purchase_item_data_supplier_item_data_id, 'N/A') as supplier_item_id,
        purchase_item_data_supplier_item_data_supplier_item_identifier as supplier_item_identifier,
        purchase_item_data_supplier_item_data_item_description as item_description,
        cast(purchase_item_data_supplier_item_data_sourcing_priority as numeric(5,2)) as sourcing_priority,
        purchase_item_data_supplier_item_data_item_url as item_url,
        cast(purchase_item_data_supplier_item_data_lead_time as numeric(5,2)) as lead_time,
        purchase_item_purchase_item_data_packaging_string as packaging_string,
        coalesce(cast(purchase_item_data_supplier_item_data_inactive as int), -2) as inactive_ind,
        to_timestamp(purchase_item_data_supplier_item_data_pricing_effective_date, 'yyyy-mm-dd') as pricing_effective_date,
        coalesce(cast(purchase_item_data_supplier_item_data_calculate_price_based_on_conversion_factor as int), -2) as calculate_price_based_on_conversion_factor_ind,
        supplier_item_data_supplier_reference_supplier_id as supplier_id,
        supplier_item_data_supplier_reference_supplier_reference_id as supplier_reference_id,
        coalesce(supplier_item_data_supplier_reference_wid, 'N/A') as supplier_wid,
        supplier_item_data_supplier_contract_reference_supplier_contract_id as supplier_contract_id,
        supplier_item_data_currency_reference_currency_id as currency_id,
        cast({{
            dbt_utils.surrogate_key([
                'purchase_item_wid',
                'purchase_item_id',
                'supplier_item_id',
                'supplier_item_identifier',
                'item_description',
                'sourcing_priority',
                'item_url',
                'lead_time',
                'packaging_string',
                'inactive_ind',
                'pricing_effective_date',
                'calculate_price_based_on_conversion_factor_ind',
                'supplier_id',
                'supplier_reference_id',
                'supplier_wid',
                'supplier_contract_id',
                'currency_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_item_supplier_item
)
select
    purchase_item_wid,
    purchase_item_id,
    supplier_item_id,
    supplier_item_identifier,
    item_description,
    sourcing_priority,
    item_url,
    lead_time,
    packaging_string,
    inactive_ind,
    pricing_effective_date,
    calculate_price_based_on_conversion_factor_ind,
    supplier_id,
    supplier_reference_id,
    supplier_wid,
    supplier_contract_id,
    currency_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    purch_itm_supp
where
    1 = 1
