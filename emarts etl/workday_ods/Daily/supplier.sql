{{
    config(
        materialized = 'incremental',
        unique_key = 'supplier_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['supplier_wid','supplier_id','supplier_name','supplier_reference_id','worktag_only_ind','customer_account_number','duns_number','submit_ind','business_entity_name','external_entity_id','business_entity_tax_id','supplier_category_id','supplier_category_wid','custom_supplier_classification_id','custom_supplier_classification_wid', 'md5', 'upd_dt', 'upd_by']
    )
}}
with suppliers as (
    select distinct
        supplier_wid,
        supplier_id,
        supplier_name,
        supplier_reference_id,
        coalesce(cast(worktag_only as int), -2) as worktag_only_ind,
        customer_account_number,
        duns_number,
        -2 as submit_ind,
        supplier_name as business_entity_name,
        null as external_entity_id,
        case when tin_type_reference_id = 'SSN_OR_ITIN' then null else tax_id end as business_entity_tax_id,
        supplier_category_id,
        supplier_category_wid,
        null as custom_supplier_classification_id,
        null as custom_supplier_classification_wid
    from
        {{source('workday_ods', 'workday_supplier')}} as workday_supplier
)
select
    supplier_wid,
    supplier_id,
    supplier_name,
    supplier_reference_id,
    worktag_only_ind,
    customer_account_number,
    duns_number,
    submit_ind,
    business_entity_name,
    external_entity_id,
    business_entity_tax_id,
    supplier_category_id,
    supplier_category_wid,
    custom_supplier_classification_id,
    custom_supplier_classification_wid,
    cast({{
        dbt_utils.surrogate_key([
            'supplier_wid',
            'supplier_id',
            'supplier_name',
            'supplier_reference_id',
            'worktag_only_ind',
            'customer_account_number',
            'duns_number',
            'submit_ind',
            'business_entity_name',
            'external_entity_id',
            'business_entity_tax_id',
            'supplier_category_id',
            'supplier_category_wid',
            'custom_supplier_classification_id',
            'custom_supplier_classification_wid'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    suppliers
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                supplier_wid = suppliers.supplier_wid
        )
    {%- endif %}
