{{ config(meta = {
    'critical': true
}) }}

with align_by_a_job_code as (
    select
        job_group_id as align_by_jcode_job_group_id,
        attribute_1_value as job_code,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where attribute_1 = 'job_code'
),

align_by_b_job_family_and_category as (
    select
        job_group_id as align_by_jf_jc_job_group_id,
        attribute_1_value as job_family,
        attribute_2_value as job_category,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where
        attribute_1 = 'job_family'
        and attribute_2 = 'job_category'
        and attribute_3 is null
),

align_by_b2_job_family_and_mgmt_lvl as (
    select
        job_group_id as align_to_job_group_id,
        attribute_1_value as job_family,
        attribute_2_value as management_level,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where
        attribute_1 = 'job_family'
        and attribute_2 = 'management_level'
        and attribute_3 is null
),

align_by_c_job_family as (
    select
        job_group_id as align_by_jf_job_group_id,
        attribute_1_value as job_family,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where
        attribute_1 = 'job_family'
        and attribute_2 is null
        and attribute_3 is null
),

set_a_rows as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_a_job_code.provider_alignment_use_ind,
        align_by_a_job_code.order_num,
        align_by_a_job_code.process_rank,
        align_by_a_job_code.align_by_jcode_job_group_id as job_group_id,
        align_by_a_job_code.root_job_hierarchy
    from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join align_by_a_job_code on stg_job_profile_workday_plus_nursing.job_code = align_by_a_job_code.job_code
),

set_b_rows as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_b_job_family_and_category.provider_alignment_use_ind,
        align_by_b_job_family_and_category.order_num,
        align_by_b_job_family_and_category.process_rank,
        align_by_b_job_family_and_category.align_by_jf_jc_job_group_id as job_group_id,
        align_by_b_job_family_and_category.root_job_hierarchy
    from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join align_by_b_job_family_and_category
        on stg_job_profile_workday_plus_nursing.job_family = align_by_b_job_family_and_category.job_family
        and stg_job_profile_workday_plus_nursing.job_category_name
            = align_by_b_job_family_and_category.job_category
),

set_b2_rows as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_b2_job_family_and_mgmt_lvl.provider_alignment_use_ind,
        align_by_b2_job_family_and_mgmt_lvl.order_num,
        align_by_b2_job_family_and_mgmt_lvl.process_rank,
        align_by_b2_job_family_and_mgmt_lvl.align_to_job_group_id as job_group_id,
        align_by_b2_job_family_and_mgmt_lvl.root_job_hierarchy
    from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join align_by_b2_job_family_and_mgmt_lvl
        on stg_job_profile_workday_plus_nursing.job_family = align_by_b2_job_family_and_mgmt_lvl.job_family
        and stg_job_profile_workday_plus_nursing.management_level
            = align_by_b2_job_family_and_mgmt_lvl.management_level
),

set_c_rows as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_c_job_family.provider_alignment_use_ind,
        align_by_c_job_family.order_num,
        align_by_c_job_family.process_rank,
        align_by_c_job_family.align_by_jf_job_group_id as job_group_id,
        align_by_c_job_family.root_job_hierarchy
    from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join align_by_c_job_family
        on stg_job_profile_workday_plus_nursing.job_family = align_by_c_job_family.job_family
)

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_a_rows

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_b_rows

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_b2_rows

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_c_rows
