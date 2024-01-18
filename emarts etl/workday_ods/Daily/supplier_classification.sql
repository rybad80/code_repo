{{
    config(
        materialized = 'incremental',
        unique_key = 'supplier_classification_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['supplier_classification_wid', 'supplier_classification_id', 'supplier_classification_name', 'external_site_ind', 'inactive_ind', 'country_alpha3_cd', 'country_alpha2_cd', 'country_numeric3_cd', 'md5', 'upd_dt', 'upd_by']
    )
}}
with sup_class as (
    select distinct
        supplier_classification_reference_wid as supplier_classification_wid,
        supplier_classification_reference_custom_supplier_classification_id as supplier_classification_id,
        supplier_classification_data_supplier_classification_name as supplier_classification_name,
        coalesce(cast(supplier_classification_data_external_site as int), -2) as external_site_ind,
        coalesce(cast(supplier_classification_data_inactive as int), -2) as inactive_ind,
        country_reference_iso_3166_1_alpha_3_code as country_alpha3_cd,
        country_reference_iso_3166_1_alpha_2_code as country_alpha2_cd,
        country_reference_iso_3166_1_numeric_3_code as country_numeric3_cd,
        cast({{
            dbt_utils.surrogate_key([
                'supplier_classification_wid',
                'supplier_classification_id',
                'supplier_classification_name',
                'external_site_ind',
                'inactive_ind',
                'country_alpha3_cd',
                'country_alpha2_cd',
                'country_numeric3_cd'
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
    supplier_classification_wid,
    supplier_classification_id,
    supplier_classification_name,
    external_site_ind,
    inactive_ind,
    country_alpha3_cd,
    country_alpha2_cd,
    country_numeric3_cd,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    sup_class
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                supplier_classification_wid = sup_class.supplier_classification_wid
        )
    {%- endif %}
