select distinct
    revenue_category_hierarchy_reference_wid as revenue_category_hierarchy_wid,
    revenue_category_hierarchy_reference_revenue_category_hierarchy_id as revenue_category_hierarchy_id,
    revenue_category_hierarchy_data_revenue_category_hierarchy_name as revenue_category_hierarchy_name,
    revenue_categories_included_reference_wid as revenue_category_wid,
    revenue_category.revenue_category_id,
    cast({{
        dbt_utils.surrogate_key([
            'revenue_category_hierarchy_wid',
            'revenue_category_hierarchy_id',
            'revenue_category_hierarchy_name',
            'revenue_category_wid',
            'revenue_category_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_revenue_category_hierarchies')}} as get_revenue_category_hierarchies
left join
    {{ref('revenue_category')}} as revenue_category
        on coalesce(get_revenue_category_hierarchies.revenue_categories_included_reference_wid, '0') = coalesce(revenue_category.revenue_category_wid, '0')
where
    1 = 1
    and get_revenue_category_hierarchies.revenue_categories_included_reference_wid is not null

