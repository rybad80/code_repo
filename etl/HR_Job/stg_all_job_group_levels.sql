{{ config(meta = {
    'critical': true
}) }}

with level_5 as (
    select
        job_group_id as level_5_id,
        job_group_level,
        job_group_name as level_5_name,
        job_group_parent
    from {{ ref('lookup_job_group_hierarchy') }}
    where job_group_level = 5
),

level_4 as (
    select
        job_group_id as level_4_id,
        job_group_level,
        job_group_name as level_4_name,
        job_group_parent
    from {{ ref('lookup_job_group_hierarchy') }}
    where job_group_level = 4
),

level_4_and_5 as (
    select
        level_4.job_group_parent,
        level_4.level_4_id,
        level_4.level_4_name,
        level_5.level_5_id,
        level_5.level_5_name,
        5 as end_level,
        1 as leaf_node,
        level_5.level_5_id as job_group_id
    from level_4
    inner join level_5 on level_4.level_4_id = level_5.job_group_parent

    union

    select
        level_4.job_group_parent,
        level_4_id,
        level_4_name,
        null as level_5_id,
        null as level_5_name,
        4 as end_level,
        0 as leaf_node,
        level_4.level_4_id as job_group_id
    from level_4
    inner join level_5 on level_4.level_4_id = level_5.job_group_parent

    union

    select
        level_4.job_group_parent,
        level_4_id,
        level_4_name,
        null as level_5_id,
        null as level_5_name,
        4 as end_level,
        1 as leaf_node,
        level_4.level_4_id as job_group_id
    from level_4
    left join level_5 on level_4.level_4_id = level_5.job_group_parent
    where level_5.job_group_parent is null
),

level_3 as (
    select
        job_group_id as level_3_id,
        job_group_level,
        job_group_name as level_3_name,
        job_group_parent
    from {{ ref('lookup_job_group_hierarchy') }}
    where job_group_level = 3
),

level_3_added as (
    select
        level_3.job_group_parent,
        level_3.level_3_id,
        level_3.level_3_name,
        level_4_id,
        level_4_name,
        level_5_id,
        level_5_name,
        end_level,
        leaf_node,
        job_group_id
    from level_3
    inner join level_4_and_5 on level_3.level_3_id = level_4_and_5.job_group_parent

    union

    select
        level_3.job_group_parent,
        level_3_id,
        level_3_name,
        null as level_4_id,
        null as level_4_name,
        null as level_5_id,
        null as level_5_name,
        3 as end_level,
        0 as leaf_node,
        level_3_id as job_group_id
    from level_3
    inner join level_4_and_5 on level_3.level_3_id = level_4_and_5.job_group_parent

    union

    select
        level_3.job_group_parent,
        level_3_id,
        level_3_name,
        null as level_4_id,
        null as level_4_name,
        null as level_5_id,
        null as level_5_name,
        3 as end_level,
        1 as leaf_node,
        level_3_id as job_group_id
    from level_3
    left join level_4_and_5 on level_3.level_3_id = level_4_and_5.job_group_parent
    where level_4_and_5.job_group_parent is null
),

level_2 as (
    select
        job_group_id as level_2_id,
        job_group_level,
        job_group_name as level_2_name,
        job_group_parent
    from {{ ref('lookup_job_group_hierarchy') }}
    where job_group_level = 2
),

level_1 as (
    select
        job_group_id as level_1_id,
        job_group_level,
        job_group_name as level_1_name,
        job_group_parent
    from {{ ref('lookup_job_group_hierarchy') }}
    where job_group_level = 1
),

level_2_added as (
    select
        level_2.job_group_parent,
        level_2.level_2_id,
        level_2.level_2_name,
        level_3_added.level_3_id,
        level_3_added.level_3_name,
        level_3_added.level_4_id,
        level_3_added.level_4_name,
        level_3_added.level_5_id,
        level_3_added.level_5_name,
        level_3_added.end_level,
        level_3_added.leaf_node,
        level_3_added.job_group_id
    from level_2
    inner join level_3_added on level_2.level_2_id = level_3_added.job_group_parent

    union

    select
        level_2.job_group_parent,
        level_2.level_2_id,
        level_2.level_2_name,
        null as level_3_id,
        null as level_3_name,
        null as level_4_id,
        null as level_4_name,
        null as level_5_id,
        null as level_5_name,
        2 as end_level,
        0 as leaf_node,
        level_2.level_2_id as job_group_id
    from level_2
    inner join level_3_added on level_2.level_2_id = level_3_added.job_group_parent

    union

    select
        level_2.job_group_parent,
        level_2.level_2_id,
        level_2.level_2_name,
        null as level_3_id,
        null as level_3_name,
        null as level_4_id,
        null as level_4_name,
        null as level_5_id,
        null as level_5_name,
        2 as end_level,
        1 as leaf_node,
        level_2.level_2_id as job_group_id
    from level_2
    left join level_3_added on level_2.level_2_id = level_3_added.job_group_parent
    where level_3_added.job_group_parent is null
)

select
    level_1.job_group_parent,
    level_1.level_1_id,
    level_1.level_1_name,
    level_2_added.level_2_id,
    level_2_added.level_2_name,
    level_2_added.level_3_id,
    level_2_added.level_3_name,
    level_2_added.level_4_id,
    level_2_added.level_4_name,
    level_2_added.level_5_id,
    level_2_added.level_5_name,
    level_2_added.end_level,
    level_2_added.leaf_node,
    level_2_added.job_group_id
from level_1
inner join level_2_added on level_1.level_1_id = level_2_added.job_group_parent

union

select
    level_1.job_group_parent,
    level_1.level_1_id,
    level_1.level_1_name,
    null as level_2_id,
    null as level_2_name,
    null as level_3_id,
    null as level_3_name,
    null as level_4_id,
    null as level_4_name,
    null as level_5_id,
    null as level_5_name,
    1 as end_level,
    0 as leaf_node,
    level_1.level_1_id as job_group_id
from level_1
inner join level_2_added on level_1.level_1_id = level_2_added.job_group_parent

union

select
    level_1.job_group_parent,
    level_1.level_1_id,
    level_1.level_1_name,
    null as level_2_id,
    null as level_2_name,
    null as level_3_id,
    null as level_3_name,
    null as level_4_id,
    null as level_4_name,
    null as level_5_id,
    null as level_5_name,
    2 as end_level,
    1 as leaf_node,
    level_1.level_1_id as job_group_id
from level_1
left join level_2_added on level_1.level_1_id = level_2_added.job_group_parent
where level_2_added.job_group_parent is null
