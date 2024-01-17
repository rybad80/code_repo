{{ config(meta = {
    'critical': true
}) }}

--     etl/Nursing/stg_job_group_s3_def_job_family_group_and_category.sql
with align_by_d_job_family_group_and_category as (
    select
        job_group_id as align_by_jfg_jc_job_group_id,
        attribute_1_value as job_family_group,
        attribute_2_value as job_category,
       root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where attribute_1 = 'job_family_group'
        and attribute_2 = 'job_category'
        and attribute_3 is null
),

align_by_e_job_family_group as (
    select
        job_group_id as align_by_jfg_job_group_id,
        attribute_1_value as job_family_group,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where
        attribute_1 = 'job_family_group'
        and attribute_2 is null
        and attribute_3 is null
),

align_by_f_job_category as (
    select
        job_group_id as align_by_jc_job_group_id,
        attribute_1_value as job_category,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
	where
        attribute_1 = 'job_category'
        and attribute_2 is null
        and attribute_3 is null
),

set_d_rows_job_family_grp_catg as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_d_job_family_group_and_category.provider_alignment_use_ind,
        align_by_d_job_family_group_and_category.order_num,
        align_by_d_job_family_group_and_category.process_rank,
        align_by_d_job_family_group_and_category.align_by_jfg_jc_job_group_id as job_group_id,
        align_by_d_job_family_group_and_category.root_job_hierarchy
 from {{ref('stg_job_profile_workday_plus_nursing')}}  as stg_job_profile_workday_plus_nursing
    inner join align_by_d_job_family_group_and_category
        on stg_job_profile_workday_plus_nursing.job_family_group
            = align_by_d_job_family_group_and_category.job_family_group
        and stg_job_profile_workday_plus_nursing.job_category_name
            = align_by_d_job_family_group_and_category.job_category
),

set_e_rows_job_family_grp as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_e_job_family_group.provider_alignment_use_ind,
        align_by_e_job_family_group.order_num,
        align_by_e_job_family_group.process_rank,
        align_by_e_job_family_group.align_by_jfg_job_group_id as job_group_id,
        align_by_e_job_family_group.root_job_hierarchy
 from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join align_by_e_job_family_group
        on stg_job_profile_workday_plus_nursing.job_family_group = align_by_e_job_family_group.job_family_group
),

set_f_rows_job_catg as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_f_job_category.provider_alignment_use_ind,
        align_by_f_job_category.order_num,
        align_by_f_job_category.process_rank,
        align_by_f_job_category.align_by_jc_job_group_id as job_group_id,
        align_by_f_job_category.root_job_hierarchy
  from {{ref('stg_job_profile_workday_plus_nursing')}}  as stg_job_profile_workday_plus_nursing
   inner join align_by_f_job_category
        on stg_job_profile_workday_plus_nursing.job_category_name = align_by_f_job_category.job_category
)

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_d_rows_job_family_grp_catg

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_e_rows_job_family_grp

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_f_rows_job_catg
