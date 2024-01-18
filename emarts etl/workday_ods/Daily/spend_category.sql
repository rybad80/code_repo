{{ config(
    materialized = 'incremental',
    unique_key = 'spend_category_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['spend_category_wid','spend_category_id','spend_category_name','procurement_usage_ind','expense_usage_ind','allocate_freight_ind','allocate_other_charges_ind','track_items_ind','stock_items_ind','intangible_reference_ind','lease_ind','inactive_ind','description','commodity_code', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    get_resource_categories.resource_category_reference_wid as spend_category_wid,
    get_resource_categories.resource_category_reference_spend_category_id as spend_category_id,
    get_resource_categories.resource_category_data_resource_category_name as spend_category_name,
    coalesce(cast(get_resource_categories.resource_category_data_procurement_usage as int), -2) as procurement_usage_ind,
    coalesce(cast(get_resource_categories.resource_category_data_expense_usage as int), -2) as expense_usage_ind,
    coalesce(cast(get_resource_categories.resource_category_data_allocate_freight as int), -2) as allocate_freight_ind,
    coalesce(cast(get_resource_categories.resource_category_data_allocate_other_charges as int), -2) as allocate_other_charges_ind,
    coalesce(cast(get_resource_categories.resource_category_data_track_items as int), -2) as track_items_ind,
    coalesce(cast(get_resource_categories.resource_category_data_stock_items as int), -2) as stock_items_ind,
    coalesce(cast(get_resource_categories.resource_category_data_intangible_reference as int), -2) as intangible_reference_ind,
    coalesce(cast(get_resource_categories.resource_category_data_lease as int), -2) as lease_ind,
    coalesce(cast(get_resource_categories.resource_category_data_inactive as int), -2) as inactive_ind,
    cast(get_resource_categories.resource_category_data_description as character varying(150)) as description,
    null as commodity_code,
    cast({{
        dbt_utils.surrogate_key([
            'spend_category_wid',
            'spend_category_id',
            'spend_category_name',
            'procurement_usage_ind',
            'expense_usage_ind',
            'allocate_freight_ind',
            'allocate_other_charges_ind',
            'track_items_ind',
            'stock_items_ind',
            'intangible_reference_ind',
            'lease_ind',
            'inactive_ind',
            'description',
            'commodity_code'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_resource_categories')}} as get_resource_categories
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                spend_category_wid = get_resource_categories.resource_category_reference_wid
        )
    {%- endif %}
