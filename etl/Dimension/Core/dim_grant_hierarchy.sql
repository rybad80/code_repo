with grant_hier_details
as (
select
    workday_grant_hierarchy_child.grant_reference_id,
    workday_grant_hierarchy_child.grant_name,
    workday_grant_hierarchy_child.grant_wid,
    workday_grant_hierarchy_child.inactive,
    workday_grant_hierarchy_child.grant_hierarchy_top_level_name,
    max(case when workday_grant_hierarchy_child.grant_hierarchy_parent_name is null
    then workday_grant_hierarchy_child.grant_hierarchy_id else null end) as grant_hierarchy_level1_id,
    max(case when workday_grant_hierarchy_child.grant_hierarchy_parent_name is null
    then workday_grant_hierarchy_child.grant_hierarchy_name else null end) as grant_hierarchy_level1_name,
    max(case when workday_grant_hierarchy_child.hier_level_num = 1
    then workday_grant_hierarchy_child.grant_hierarchy_id else null end) as grant_hierarchy_level2_id,
    max(case when workday_grant_hierarchy_child.hier_level_num = 1
    then workday_grant_hierarchy_child.grant_hierarchy_name else null end) as grant_hierarchy_level2_name,
    max(case when workday_grant_hierarchy_child.hier_level_num = 2
    then workday_grant_hierarchy_child.grant_hierarchy_id else null end) as grant_hierarchy_level3_id,
    max(case when workday_grant_hierarchy_child.hier_level_num = 2
    then workday_grant_hierarchy_child.grant_hierarchy_name else null end) as grant_hierarchy_level3_name,
    max(case when workday_grant_hierarchy_child.hier_level_num = 3
    then workday_grant_hierarchy_child.grant_hierarchy_id else null end) as grant_hierarchy_level4_id,
    max(case when workday_grant_hierarchy_child.hier_level_num = 3
    then workday_grant_hierarchy_child.grant_hierarchy_name else null end) as grant_hierarchy_level4_name,
    max(case when workday_grant_hierarchy_child.hier_level_num = 4
    then workday_grant_hierarchy_child.grant_hierarchy_id else null end) as grant_hierarchy_level5_id,
    max(case when workday_grant_hierarchy_child.hier_level_num = 4
    then workday_grant_hierarchy_child.grant_hierarchy_name else null end) as grant_hierarchy_level5_name,
    max(case when workday_grant_hierarchy_child.hier_level_num = 5
    then workday_grant_hierarchy_child.grant_hierarchy_id else null end) as grant_hierarchy_level6_id,
    max(case when workday_grant_hierarchy_child.hier_level_num = 5
    then workday_grant_hierarchy_child.grant_hierarchy_name else null end) as grant_hierarchy_level6_name,
    max(workday_grant_hierarchy_child.upd_dt) as upd_dt
from
    {{source('workday_ods', 'workday_grant_hierarchy')}} as workday_grant_hierarchy_child
left join {{source('workday_ods', 'workday_grant_hierarchy')}} as workday_grant_hierarchy_parent
on
    workday_grant_hierarchy_child.grant_hierarchy_parent_name = workday_grant_hierarchy_parent.grant_hierarchy_name
    and workday_grant_hierarchy_child.grant_wid = workday_grant_hierarchy_parent.grant_wid
group by
    workday_grant_hierarchy_child.grant_reference_id,
    workday_grant_hierarchy_child.grant_name,
    workday_grant_hierarchy_child.grant_wid,
    workday_grant_hierarchy_child.inactive,
    workday_grant_hierarchy_child.grant_hierarchy_top_level_name
), 
grant_hier
as (
select
    {{
        dbt_utils.surrogate_key([
            'grant_hier_details.grant_wid',
            'grant_hier_details.grant_hierarchy_top_level_name'
        ])
    }} as grant_hier_key,
    grant_hier_details.grant_reference_id as grant_id,
    grant_hier_details.grant_name,
    grant_hier_details.grant_wid,
    grant_hier_details.inactive as grant_inactive,
    grant_hier_details.grant_hierarchy_top_level_name,
    grant_hier_details.grant_hierarchy_level1_id,
    grant_hier_details.grant_hierarchy_level1_name,
    grant_hier_details.grant_hierarchy_level2_id,
    grant_hier_details.grant_hierarchy_level2_name,
    grant_hier_details.grant_hierarchy_level3_id,
    grant_hier_details.grant_hierarchy_level3_name,
    grant_hier_details.grant_hierarchy_level4_id,
    grant_hier_details.grant_hierarchy_level4_name,
    grant_hier_details.grant_hierarchy_level5_id,
    grant_hier_details.grant_hierarchy_level5_name,
    grant_hier_details.grant_hierarchy_level6_id,
    grant_hier_details.grant_hierarchy_level6_name,
    {{
        dbt_utils.surrogate_key([
        'grant_hier_details.grant_reference_id',
        'grant_hier_details.grant_name',
        'grant_hier_details.grant_wid',
        'grant_hier_details.inactive',
        'grant_hier_details.grant_hierarchy_top_level_name',
        'grant_hier_details.grant_hierarchy_level1_id',
        'grant_hier_details.grant_hierarchy_level1_name',
        'grant_hier_details.grant_hierarchy_level2_id',
        'grant_hier_details.grant_hierarchy_level2_name',
        'grant_hier_details.grant_hierarchy_level3_id',
        'grant_hier_details.grant_hierarchy_level3_name',
        'grant_hier_details.grant_hierarchy_level4_id',
        'grant_hier_details.grant_hierarchy_level4_name',
        'grant_hier_details.grant_hierarchy_level5_id',
        'grant_hier_details.grant_hierarchy_level5_name',
        'grant_hier_details.grant_hierarchy_level6_id',
        'grant_hier_details.grant_hierarchy_level6_name'
        ])
    }} as hash_value,
    'WORKDAY'
    || '~'
    || grant_hier_details.grant_reference_id
    || '~'
    || grant_hier_details.grant_hierarchy_top_level_name as integration_id,
    current_timestamp as create_date,
    'WORKDAY' as create_by,
    current_timestamp as update_date,
    'WORKDAY' as update_by
from
    grant_hier_details
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
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    'NA',
    0,
    'NA',
    CURRENT_TIMESTAMP,
    'DEFAULT',
    CURRENT_TIMESTAMP, 
    'DEFAULT'    
)
select 
    grant_hier.grant_hier_key,
    grant_hier.grant_id,
    grant_hier.grant_name,
    grant_hier.grant_wid,
    grant_hier.grant_inactive,
    grant_hier.grant_hierarchy_top_level_name,
    grant_hier.grant_hierarchy_level1_id,
    grant_hier.grant_hierarchy_level1_name,
    grant_hier.grant_hierarchy_level2_id,
    grant_hier.grant_hierarchy_level2_name,
    grant_hier.grant_hierarchy_level3_id,
    grant_hier.grant_hierarchy_level3_name,
    grant_hier.grant_hierarchy_level4_id,
    grant_hier.grant_hierarchy_level4_name,
    grant_hier.grant_hierarchy_level5_id,
    grant_hier.grant_hierarchy_level5_name,
    grant_hier.grant_hierarchy_level6_id,
    grant_hier.grant_hierarchy_level6_name,
    grant_hier.hash_value,
    grant_hier.integration_id,
    grant_hier.create_date,
    grant_hier.create_by,
    grant_hier.update_date,
    grant_hier.update_by
from 
    grant_hier    
where 1 = 1 
