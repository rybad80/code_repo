{{ config(meta = {
    'critical': true
}) }}

/* job_group_levels_nursing
adds meaningful fields to the enterprise job_group_levels dataset
that will help with building and displaying nursing metrics,
nursing_job_group_sort_num takes into account nursing preferred list order
and breaking ties
*/
with
get_sort_mult as (
    select
        job_group_levels.job_group_sort_num
            + add_field.nursing_job_group_sort_addend as nursing_sort,
        1 as break_tie_increment,
        count(*) as tie_count
    from
        {{ ref('job_group_levels') }} as job_group_levels
        inner join {{ ref('stg_nursing_job_group_levels') }} as add_field
            on job_group_levels.job_group_id = add_field.job_group_id
    where
        leaf_node_ind = 1
    group by
        nursing_sort having count(*) > 1
)

select
    job_group_levels.job_group_id,
    job_group_levels.job_group_sort_num
        + add_field.nursing_job_group_sort_addend
        + coalesce((get_sort_mult.break_tie_increment
            * dense_rank() over (partition by nursing_sort
                order by job_group_levels.job_group_id)),
                0)
        as nursing_job_group_sort_num,
    job_group_levels.root_job_hierarchy,
    job_group_levels.job_group_name,
    job_group_levels.level_1_id,
    job_group_levels.level_2_id,
    job_group_levels.level_3_id,
    job_group_levels.level_4_id,
    job_group_levels.level_5_id,
    job_group_levels.end_level,
    job_group_levels.leaf_node_ind,
    job_group_levels.job_group_parent,
    job_group_levels.job_group_granularity_path,
    add_field.staff_nurse_ind,
    coalesce(
        add_field.job_group_level_4_id,
        job_group_levels.job_group_id,
        'job group TBD') as nursing_job_rollup,
    add_field.variable_job_ind,
    job_group_levels.job_group_name_granularity_path,
    job_group_levels.level_1_name,
    job_group_levels.level_2_name,
    job_group_levels.level_3_name,
    job_group_levels.level_4_name,
    job_group_levels.level_5_name,
    job_group_levels.job_group_desc,
    job_group_levels.job_group_sort_num,
    add_field.nursing_job_group_sort_addend,
    job_group_levels.level_1_sort_factor,
    job_group_levels.level_2_sort_factor,
    job_group_levels.level_3_sort_factor,
    job_group_levels.level_4_sort_factor,
    job_group_levels.level_5_sort_factor
from
    {{ ref('job_group_levels') }} as job_group_levels
    inner join {{ ref('stg_nursing_job_group_levels') }} as add_field
        on job_group_levels.job_group_id = add_field.job_group_id
    left join get_sort_mult
        on job_group_levels.job_group_sort_num
            + add_field.nursing_job_group_sort_addend = get_sort_mult.nursing_sort
