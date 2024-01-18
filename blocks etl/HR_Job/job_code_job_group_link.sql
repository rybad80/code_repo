{{ config(meta = {
    'critical': true
}) }}

select
    job_group_all.job_code,
    lookup_job_group_hierarchy.job_group_name as root_job_hierarchy,
    job_group_all.granularity_level,
    job_group_all.job_group_level_name as job_group_granularity_name,
    job_group_all.end_level as end_granularity_level,
    case when granularity_level = end_granularity_level
        then 1 else 0 end as leaf_node_ind,
    case when granularity_level = end_granularity_level - 1
        then 1 else 0 end as direct_parent_ind,
    job_group_all.root_id as job_group_root_id,
    job_group_all.granularity_level_id as job_group_id
from {{ref('stg_job_group_build_all_levels_code')}} as job_group_all
inner join {{ref('lookup_job_group_hierarchy')}} as lookup_job_group_hierarchy
    on job_group_all.root_id = lookup_job_group_hierarchy.job_group_id
