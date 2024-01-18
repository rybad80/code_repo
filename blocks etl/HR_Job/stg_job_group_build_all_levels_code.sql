{{ config(meta = {
    'critical': true
}) }}

with all_levels_for_leaf as (
    select
        job_group_levels.job_group_name,
        job_group_levels.level_1_name,
        job_group_levels.level_2_name,
        job_group_levels.level_3_name,
        job_group_levels.level_4_name,
        job_group_levels.level_5_name,
        job_group_levels.end_level,
        job_group_levels.leaf_node_ind,
        job_code_profile.job_code,
        job_code_profile.provider_job_group_id as job_group_id,
        job_group_levels.level_1_id,
        job_group_levels.level_2_id,
        job_group_levels.level_3_id,
        job_group_levels.level_4_id,
        job_group_levels.level_5_id
    from {{ ref('job_code_profile') }} as job_code_profile
    inner join {{ ref('job_group_levels') }} as job_group_levels
        on job_code_profile.provider_job_group_id  = job_group_levels.job_group_id

    union all

    select
        job_group_levels.job_group_name,
        job_group_levels.level_1_name,
        job_group_levels.level_2_name,
        job_group_levels.level_3_name,
        job_group_levels.level_4_name,
        job_group_levels.level_5_name,
        job_group_levels.end_level,
        job_group_levels.leaf_node_ind,
        job_code_profile_rn_alt.job_code,
        job_code_profile_rn_alt.rn_alt_job_group_id as job_group_id,
        job_group_levels.level_1_id,
        job_group_levels.level_2_id,
        job_group_levels.level_3_id,
        job_group_levels.level_4_id,
        job_group_levels.level_5_id
    from {{ ref('job_code_profile') }} as job_code_profile_rn_alt
    left join {{ ref('job_group_levels') }} as job_group_levels
        on job_code_profile_rn_alt.rn_alt_job_group_id  = job_group_levels.job_group_id

    union all

    select
        job_group_levels.job_group_name,
        job_group_levels.level_1_name,
        job_group_levels.level_2_name,
        job_group_levels.level_3_name,
        job_group_levels.level_4_name,
        job_group_levels.level_5_name,
        job_group_levels.end_level,
        job_group_levels.leaf_node_ind,
        additional_job_group.job_code,
        additional_job_group.job_group_id,
        job_group_levels.level_1_id,
        job_group_levels.level_2_id,
        job_group_levels.level_3_id,
        job_group_levels.level_4_id,
        job_group_levels.level_5_id
    from {{ ref('job_code_role_clarity_job_group') }} as additional_job_group
    left join {{ ref('job_group_levels') }} as job_group_levels
        on additional_job_group.job_group_id  = job_group_levels.job_group_id
),

job_code_level_1_granularity as (
    select
        job_code,
        1 as granularity_level,
        level_1_id as root_id,
        end_level,
        level_1_id as granularity_level_id,
        level_1_name as job_group_level_name
    from all_levels_for_leaf
    where level_1_name is not null
),

job_code_level_2_granularity as (
    select
        job_code,
        2 as granularity_level,
        level_1_id as root_id,
        end_level,
        level_2_id as granularity_level_id,
        level_2_name as job_group_level_name
    from all_levels_for_leaf
    where level_2_name is not null
),

job_code_level_3_granularity as (
    select
        job_code,
        3 as granularity_level,
        level_1_id as root_id,
        end_level,
        level_3_id as granularity_level_id,
        level_3_name as job_group_level_name
    from all_levels_for_leaf
    where level_3_name is not null
),

job_code_level_4_granularity as (
    select
        job_code,
        4 as granularity_level,
        level_1_id as root_id,
        end_level,
        level_4_id as granularity_level_id,
        level_4_name as job_group_level_name
    from all_levels_for_leaf
    where level_4_name is not null
),

job_code_level_5_granularity as (
    select
        job_code,
        5 as granularity_level,
        level_1_id as root_id,
        end_level,
        level_5_id as granularity_level_id,
        level_5_name as job_group_level_name
    from all_levels_for_leaf
    where level_5_name is not null
)

select
    job_code,
    granularity_level,
    root_id,
    end_level,
    granularity_level_id,
    job_group_level_name
from job_code_level_1_granularity

union all

select
    job_code,
    granularity_level,
    root_id,
    end_level,
    granularity_level_id,
    job_group_level_name
from job_code_level_2_granularity

union all

select
    job_code,
    granularity_level,
    root_id,
    end_level,
    granularity_level_id,
    job_group_level_name
from job_code_level_3_granularity

union all

select
    job_code,
    granularity_level,
    root_id,
    end_level,
    granularity_level_id,
    job_group_level_name
from job_code_level_4_granularity

union all

select
    job_code,
    granularity_level,
    root_id,
    end_level,
    granularity_level_id,
    job_group_level_name
from job_code_level_5_granularity
