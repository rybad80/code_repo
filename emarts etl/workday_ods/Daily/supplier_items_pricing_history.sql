{{
    config(
        materialized = 'incremental',
        unique_key = 'supplier_item_pricing_history_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['supplier_item_pricing_history_wid','supplier_item_wid','supplier_item_id','effective_date','conversion_factor','lead_time','minimum_order_quantity','just_in_time_ind','is_default_ordering_unit_of_measure_ind','pricing_not_calculated_by_conversion_factor_ind','unit_price_as_of_effective_date','active_as_of_effective_date_ind','unit_of_measure_descriptor', 'md5', 'upd_dt', 'upd_by']
    )
}}
with sup_items_pricing as (
    select distinct
        supplier_item_pricing_history_wid,
        supplier_item_wid,
        supplier_item_reference_id as supplier_item_id,
        to_date(substr(effective_date,1,10),'YYYY-MM-DD') as effective_date,
        cast(conversion_factor as numeric(26,6)) as conversion_factor,
        cast(lead_time as int) as lead_time,
        cast(minimum_order_quantity as int) as minimum_order_quantity,
        coalesce(cast(just_in_time as int), -2) as just_in_time_ind,
        coalesce(cast(is_default_ordering_unit_of_measure as int), -2) as is_default_ordering_unit_of_measure_ind,
        coalesce(cast(pricing_not_calculated_by_conversion_factor as int), -2) as pricing_not_calculated_by_conversion_factor_ind,
        cast(unit_price_as_of_effective_date as numeric(26,6)) as unit_price_as_of_effective_date,
        coalesce(cast(active_as_of_effective_date as int), -2) as active_as_of_effective_date_ind,
        pricing_history_unit_of_measure as unit_of_measure_descriptor
    from
        {{source('workday_ods', 'workday_supplier_items')}} as workday_supplier_items
)
select
    supplier_item_pricing_history_wid,
    supplier_item_wid,
    supplier_item_id,
    effective_date,
    conversion_factor,
    lead_time,
    minimum_order_quantity,
    just_in_time_ind,
    is_default_ordering_unit_of_measure_ind,
    pricing_not_calculated_by_conversion_factor_ind,
    unit_price_as_of_effective_date,
    active_as_of_effective_date_ind,
    unit_of_measure_descriptor,
    cast({{
        dbt_utils.surrogate_key([
            'supplier_item_pricing_history_wid',
            'supplier_item_wid',
            'supplier_item_id',
            'effective_date',
            'conversion_factor',
            'lead_time',
            'minimum_order_quantity',
            'just_in_time_ind',
            'is_default_ordering_unit_of_measure_ind',
            'pricing_not_calculated_by_conversion_factor_ind',
            'unit_price_as_of_effective_date',
            'active_as_of_effective_date_ind',
            'unit_of_measure_descriptor'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    sup_items_pricing
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                supplier_item_pricing_history_wid = sup_items_pricing.supplier_item_pricing_history_wid
        )
    {%- endif %}
