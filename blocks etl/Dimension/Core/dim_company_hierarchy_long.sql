{{
    config(
        materialized = 'incremental',
        unique_key = ['company_wid', 'top_level', 'hier_level_num'],
        incremental_strategy = 'merge',
        merge_update_columns = [ 'company_id', 'company_name', 'company_code' ,'top_level',
            'company_hierarchy_level', 'company_hierarchy_level_wid',
            'company_hierarchy_level_id', 'hier_level_num',
            'update_date', 'hash_value', 'integration_id'],
        meta={
            'critical': true
        }
    )
}}
with company_hier_details
as (
select
    company.company_wid,
    company.company_id,
    company.company_name,
    company.company_code,
	company_hierarchy_levels.*
from
    {{source('workday_ods', 'company_company_hierarchy')}} as company_company_hierarchy,
    {{source('workday_ods', 'company_hierarchy_levels')}} as company_hierarchy_levels,
    {{source('workday_ods', 'company')}} as company
where
    company_company_hierarchy.company_hierarchy_id = company_hierarchy_levels.company_hierarchy_id
    and company.company_id = company_company_hierarchy.company_id
),
company_hier_long
as (
select
    company_wid,
    company_id,
    company_name,
    company_code,
    company_hierarchy_level1_name as top_level,
    company_hierarchy_level1_name as company_hierarchy_level,
    company_hierarchy_level1_wid as company_hierarchy_level_wid,
    company_hierarchy_level1_id as company_hierarchy_level_id,
    1 as hier_level,
    create_by,
    upd_by
from
    company_hier_details
--
union all
--
select
    company_wid,
    company_id,
    company_name,
    company_code,
    company_hierarchy_level1_name as top_level,
    company_hierarchy_level2_name as company_hierarchy_level,
    company_hierarchy_level2_wid as company_hierarchy_level_wid,
    company_hierarchy_level2_id as company_hierarchy_level_id,
    2 as hier_level,
    create_by,
    upd_by
from
    company_hier_details
where
    company_hierarchy_level2_name is not null
--
union all
--
select
    company_wid,
    company_id,
    company_name,
    company_code,
    company_hierarchy_level1_name as top_level,
    company_hierarchy_level3_name as company_hierarchy_level,
    company_hierarchy_level3_wid as company_hierarchy_level_wid,
    company_hierarchy_level3_id as company_hierarchy_level_id,
    3 as hier_level,
    create_by,
    upd_by
from
    company_hier_details
where
    company_hierarchy_level3_name is not null
),
company_hier
as (
select
    {{
        dbt_utils.surrogate_key([
            'company_wid',
            'top_level',
            'hier_level'
        ])
    }} as company_hier_key,
    company_wid,
    company_id,
    company_name,
    company_code,
    top_level,
    company_hierarchy_level,
    company_hierarchy_level_wid,
    company_hierarchy_level_id,
    hier_level as hier_level_num,
    {{
        dbt_utils.surrogate_key([
            'company_wid',
	        'company_id',
            'company_name',
            'company_code',
            'top_level',
            'company_hierarchy_level',
            'company_hierarchy_level_wid',
            'company_hierarchy_level_id',
            'hier_level'
        ])
    }} as hash_value,
    create_by || '~' || company_id  || '~' || top_level || '~' || hier_level as integration_id,
    current_timestamp as create_date,
    create_by,
    current_timestamp as update_date,
    upd_by as update_by
from
    company_hier_long
where 1 = 1
--
union all
--
select
    0,
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    0,
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP,
    'DEFAULT'
)
select
    company_hier_key,
    company_wid,
    company_id,
    company_name,
    company_code,
    top_level,
    company_hierarchy_level,
    company_hierarchy_level_wid,
    company_hierarchy_level_id,
    hier_level_num,
    hash_value,
    integration_id,
    create_date,
    create_by,
    update_date,
    update_by
from
    company_hier
where 1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where company_wid = company_hier.company_wid
      and top_level = company_hier.top_level
      and hier_level_num = company_hier.hier_level_num)
{%- endif %}
