select distinct
get_spend_category_hierarchies.spend_category_hierarchy_reference_wid as spend_category_hierarchy_wid,
get_spend_category_hierarchies.spend_category_hierarchy_reference_resource_category_hierarchy_id as spend_category_hierarchy_id,
get_spend_category_hierarchies.spend_category_hierarchy_data_spend_category_hierarchy_names as spend_category_hierarchy_name,
coalesce(get_spend_category_hierarchies.included_spend_categories_reference_wid, 'N/A') as spend_category_wid,
spend_category.spend_category_id,
    cast({{
        dbt_utils.surrogate_key([
            'spend_category_hierarchy_wid',
            'spend_category_hierarchy_id',
            'spend_category_hierarchy_name',
            'spend_category_wid',
            'spend_category_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_spend_category_hierarchies')}} as get_spend_category_hierarchies
left join
    {{ref('spend_category')}} as spend_category
        on coalesce(get_spend_category_hierarchies.included_spend_categories_reference_wid, '0') = coalesce(spend_category.spend_category_wid, '0')
where
    1 = 1
