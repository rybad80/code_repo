{{ config(meta = {
    'critical': true
}) }}

select
    (1000000 * coalesce(lvl_1.job_group_sort_factor, 0))
    + (10000 * coalesce(lvl_2.job_group_sort_factor, 0))
    + (100 * coalesce(lvl_3.job_group_sort_factor, 0))
    + (10 * coalesce(lvl_4.job_group_sort_factor, 0))
    + (1 * coalesce(lvl_5.job_group_sort_factor, 0)) as job_group_sort_num,
    lvl_1.job_group_name as root_job_hierarchy,
    lookup_job_group_hierarchy.job_group_name,
    all_job_group_levels.level_1_id,
    all_job_group_levels.level_1_name,
    all_job_group_levels.level_2_id,
    all_job_group_levels.level_2_name,
    all_job_group_levels.level_3_id,
    all_job_group_levels.level_3_name,
    all_job_group_levels.level_4_id,
    all_job_group_levels.level_4_name,
    all_job_group_levels.level_5_id,
    all_job_group_levels.level_5_name,
    all_job_group_levels.end_level,
    all_job_group_levels.leaf_node as leaf_node_ind,
    all_job_group_levels.job_group_id,
    lookup_job_group_hierarchy.job_group_parent,
    lookup_job_group_hierarchy.job_group_desc,
    all_job_group_levels.level_1_id || case when all_job_group_levels.level_2_id is not null
        then ' | ' || all_job_group_levels.level_2_id
    else '' end || case when all_job_group_levels.level_3_id is not null
        then ' | ' || all_job_group_levels.level_3_id
    else '' end || case when all_job_group_levels.level_4_id is not null
        then ' | ' || all_job_group_levels.level_4_id
    else '' end || case when all_job_group_levels.level_5_id is not null
        then ' | ' || all_job_group_levels.level_5_id else '' end as job_group_granularity_path,
    all_job_group_levels.level_1_name || case when all_job_group_levels.level_2_name is not null
        then ' | ' || all_job_group_levels.level_2_name
    else '' end || case when all_job_group_levels.level_3_name is not null
        then ' | ' || all_job_group_levels.level_3_name
    else '' end || case when all_job_group_levels.level_4_name is not null
        then ' | ' || all_job_group_levels.level_4_name
    else '' end || case when all_job_group_levels.level_5_name is not null
        then ' | ' || all_job_group_levels.level_5_name else '' end as job_group_name_granularity_path,
    lvl_1.job_group_sort_factor as level_1_sort_factor,
    lvl_2.job_group_sort_factor as level_2_sort_factor,
    lvl_3.job_group_sort_factor as level_3_sort_factor,
    lvl_4.job_group_sort_factor as level_4_sort_factor,
    lvl_5.job_group_sort_factor as level_5_sort_factor
from {{ ref('stg_all_job_group_levels') }} as all_job_group_levels
inner join {{ ref('lookup_job_group_hierarchy') }} as lookup_job_group_hierarchy
    on all_job_group_levels.job_group_id = lookup_job_group_hierarchy.job_group_id
inner join {{ ref('lookup_job_group_hierarchy') }} as lvl_1
    on all_job_group_levels.level_1_id  = lvl_1.job_group_id
left join {{ ref('lookup_job_group_hierarchy') }} as lvl_2
    on  all_job_group_levels.level_2_id  = lvl_2.job_group_id
left join {{ ref('lookup_job_group_hierarchy') }} as lvl_3
    on  all_job_group_levels.level_3_id  = lvl_3.job_group_id
left join {{ ref('lookup_job_group_hierarchy') }} as lvl_4
    on  all_job_group_levels.level_4_id  = lvl_4.job_group_id
left join {{ ref('lookup_job_group_hierarchy') }} as lvl_5
    on  all_job_group_levels.level_5_id  = lvl_5.job_group_id
