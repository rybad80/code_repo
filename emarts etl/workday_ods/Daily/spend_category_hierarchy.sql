{{ config(
    materialized = 'incremental',
    unique_key = 'spend_category_hierarchy_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['spend_category_hierarchy_wid','spend_category_hierarchy_id','spend_category_hierarchy_name','enable_for_external_website_ind','commodity_code','description','inactive_ind','spend_category_hierarchy_level_id','parent_spend_category_hierarchy_wid','parent_spend_category_hierarchy_id', 'md5', 'upd_dt', 'upd_by']
) }}
select distinct
    get_spend_category_hierarchies.spend_category_hierarchy_reference_wid as spend_category_hierarchy_wid,
    get_spend_category_hierarchies.spend_category_hierarchy_reference_resource_category_hierarchy_id as spend_category_hierarchy_id,
    get_spend_category_hierarchies.spend_category_hierarchy_data_spend_category_hierarchy_names as spend_category_hierarchy_name,
    get_spend_category_hierarchies.spend_category_hierarchy_data_enable_for_external_website as enable_for_external_website_ind,
    null as commodity_code,
    get_spend_category_hierarchies.spend_category_hierarchy_data_description as description,
    get_spend_category_hierarchies.spend_category_hierarchy_data_inactive as inactive_ind,
    null as spend_category_hierarchy_level_id,
    get_spend_category_hierarchies.parent_reference_wid as parent_spend_category_hierarchy_wid,
    get_spend_category_hierarchies.parent_reference_resource_category_hierarchy_id as parent_spend_category_hierarchy_id,
    cast({{
        dbt_utils.surrogate_key([
            'spend_category_hierarchy_wid',
            'spend_category_hierarchy_id',
            'spend_category_hierarchy_name',
            'enable_for_external_website_ind',
            'commodity_code',
            'description',
            'inactive_ind',
            'spend_category_hierarchy_level_id',
            'parent_spend_category_hierarchy_wid',
            'parent_spend_category_hierarchy_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_spend_category_hierarchies')}} as get_spend_category_hierarchies
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                spend_category_hierarchy_wid = get_spend_category_hierarchies.spend_category_hierarchy_reference_wid
        )
    {%- endif %}
