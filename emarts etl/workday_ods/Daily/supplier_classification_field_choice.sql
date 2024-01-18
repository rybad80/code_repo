--MAYBE DOES NOT GET USED AND NOT NEEDED

{{
    config(
        materialized = 'incremental',
        unique_key = 'supplier_classification_field_choice_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['supplier_classification_field_choice_wid', 'supplier_classification_field_choice_id', 'line_order', 'field_choice_label', 'supplier_classification_wid', 'supplier_classification_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
with sup_class_field as (
    select distinct
        supplier_classification_field_choice_reference_wid as supplier_classification_field_choice_wid,
        supplier_classification_field_choice_replacement_data_supplier_classification_field_choice_id as supplier_classification_field_choice_id,
        supplier_classification_field_choice_replacement_data_line_order as line_order,
        supplier_classification_field_choice_replacement_data_field_choice_label as field_choice_label,
        supplier_classification_reference_wid as supplier_classification_wid,
        supplier_classification_data_supplier_classification_id as supplier_classification_id,
        cast({{
            dbt_utils.surrogate_key([
                'supplier_classification_field_choice_wid',
                'supplier_classification_field_choice_id',
                'line_order',
                'field_choice_label',
                'supplier_classification_wid',
                'supplier_classification_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_supplier_classification')}} as get_supplier_classification
)
select
    supplier_classification_field_choice_wid,
    supplier_classification_field_choice_id,
    line_order,
    field_choice_label,
    supplier_classification_wid,
    supplier_classification_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    sup_class_field
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                supplier_classification_field_choice_wid = sup_class_field.supplier_classification_field_choice_wid
        )
    {%- endif %}
