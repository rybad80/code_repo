{{
    config(
        materialized = 'incremental',
        unique_key = ['purchase_item_wid', 'purchase_item_id', 'purchase_item_unit_of_measure_data_uom_reference_wid'],
        incremental_strategy = 'merge',
        merge_update_columns = ['purchase_item_wid','purchase_item_id','purchase_item_unit_of_measure_data_uom_reference_wid','purchase_item_unit_of_measure_data_uom_reference_edi_id','purchase_item_unit_of_measure_data_uom_reference_cefact_id','conversion_factor','unit_price','default_for_ordering_ind','default_for_stocking_ind','default_for_issuing_ind','default_for_unit_of_use_ind','allow_decimal_quantities_ind','inactive_ind','md5','upd_dt','upd_by']
    )
}}

with purch_itm_uom as (
    select distinct
        purchase_item_purchase_item_reference_wid as purchase_item_wid,
        purchase_item_purchase_item_reference_purchase_item_id as purchase_item_id,
        purchase_item_unit_of_measure_data_unit_of_measure_reference_wid as purchase_item_unit_of_measure_data_uom_reference_wid,
        purchase_item_unit_of_measure_data_unit_of_measure_reference_uom_edi_code_id as purchase_item_unit_of_measure_data_uom_reference_edi_id,
        purchase_item_unit_of_measure_data_unit_of_measure_reference_un_cefact_common_code_id as purchase_item_unit_of_measure_data_uom_reference_cefact_id,
        cast(purchase_item_data_purchase_item_unit_of_measure_data_conversion_factor as numeric(21,9)) as conversion_factor,
        cast(purchase_item_data_purchase_item_unit_of_measure_data_unit_price as numeric(26,6)) as unit_price,
        coalesce(cast(purchase_item_data_purchase_item_unit_of_measure_data_default_for_ordering as int), -2) as default_for_ordering_ind,
        coalesce(cast(purchase_item_data_purchase_item_unit_of_measure_data_default_for_stocking as int), -2) as default_for_stocking_ind,
        coalesce(cast(purchase_item_data_purchase_item_unit_of_measure_data_default_for_issuing as int), -2) as default_for_issuing_ind,
        coalesce(cast(purchase_item_data_purchase_item_unit_of_measure_data_default_for_unit_of_use as int), -2) as default_for_unit_of_use_ind,
        coalesce(cast(purchase_item_data_purchase_item_unit_of_measure_data_allow_decimal_quantities as int), -2) as allow_decimal_quantities_ind,
        coalesce(cast(purchase_item_data_purchase_item_unit_of_measure_data_inactive as int), -2) as inactive_ind,
        cast({{
            dbt_utils.surrogate_key([
                'purchase_item_wid',
                'purchase_item_id',
                'purchase_item_unit_of_measure_data_uom_reference_wid',
                'purchase_item_unit_of_measure_data_uom_reference_edi_id',
                'purchase_item_unit_of_measure_data_uom_reference_cefact_id',
                'conversion_factor',
                'unit_price',
                'default_for_ordering_ind',
                'default_for_stocking_ind',
                'default_for_issuing_ind',
                'default_for_unit_of_use_ind',
                'allow_decimal_quantities_ind',
                'inactive_ind'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_purchase_items')}} as get_purchase_items
)
select
    purchase_item_wid,
    purchase_item_id,
    purchase_item_unit_of_measure_data_uom_reference_wid,
    purchase_item_unit_of_measure_data_uom_reference_edi_id,
    purchase_item_unit_of_measure_data_uom_reference_cefact_id,
    conversion_factor,
    unit_price,
    default_for_ordering_ind,
    default_for_stocking_ind,
    default_for_issuing_ind,
    default_for_unit_of_use_ind,
    allow_decimal_quantities_ind,
    inactive_ind,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    purch_itm_uom
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                purchase_item_wid = purch_itm_uom.purchase_item_wid
                and purchase_item_id = purch_itm_uom.purchase_item_id
                and purchase_item_unit_of_measure_data_uom_reference_wid = purch_itm_uom.purchase_item_unit_of_measure_data_uom_reference_wid
        )
    {%- endif %}
