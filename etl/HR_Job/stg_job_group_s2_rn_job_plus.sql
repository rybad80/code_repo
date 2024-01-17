{{ config(meta = {
    'critical': true
}) }}

with align_by_g_job_family_and_no_ncatg as (
    select
        job_group_id as align_by_g_job_group_id,
        coalesce(attribute_1_value, 'NULLvalue') as nursing_category,
        attribute_2_value as job_family,
       root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where attribute_1 = 'nursing_category'
        and attribute_2 = 'job_family'
        and attribute_3 is null
),

align_by_h_job_family_catg_rn as (
    select
        job_group_id as align_by_h_job_group_id,
        attribute_1_value as job_family,
        attribute_2_value as job_category,
        attribute_3_value as rn_job_ind,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where
        attribute_1 = 'job_family'
        and attribute_2 = 'job_category'
        and attribute_3 = 'rn_job_ind'
),

align_by_h1_rn_job_job_family as (
    select
        job_group_id as align_by_h_job_group_id,
        attribute_1_value as rn_job_ind,
        attribute_2_value as job_family,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where
        attribute_1 = 'rn_job_ind'
        and attribute_2 = 'job_family'
        and attribute_3 is null
),

align_by_h2_rn_job as (
    select
        job_group_id as align_by_h_job_group_id,
        attribute_1_value as rn_job_ind,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where
        attribute_1 = 'rn_job_ind'
        and attribute_2 is null
        and attribute_3 is null
),

set_g_rows_job_family_no_ncatg as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_g_job_family_and_no_ncatg.provider_alignment_use_ind,
        align_by_g_job_family_and_no_ncatg.order_num,
        align_by_g_job_family_and_no_ncatg.process_rank,
        align_by_g_job_family_and_no_ncatg.align_by_g_job_group_id as job_group_id,
        align_by_g_job_family_and_no_ncatg.root_job_hierarchy
 from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join align_by_g_job_family_and_no_ncatg
        on coalesce(stg_job_profile_workday_plus_nursing.nursing_category, 'NULLvalue')
            = align_by_g_job_family_and_no_ncatg.nursing_category
        and stg_job_profile_workday_plus_nursing.job_family
            = align_by_g_job_family_and_no_ncatg.job_family
),

set_h_rows_job_family_catg_rn as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_h_job_family_catg_rn.provider_alignment_use_ind,
        align_by_h_job_family_catg_rn.order_num,
        align_by_h_job_family_catg_rn.process_rank,
        align_by_h_job_family_catg_rn.align_by_h_job_group_id as job_group_id,
        align_by_h_job_family_catg_rn.root_job_hierarchy
    from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join {{ ref('lookup_nursing_category') }} as lookup_nursing_category
        on stg_job_profile_workday_plus_nursing.nursing_category = lookup_nursing_category.for_nursing_category
    inner join align_by_h_job_family_catg_rn
        on stg_job_profile_workday_plus_nursing.job_family = align_by_h_job_family_catg_rn.job_family
        and stg_job_profile_workday_plus_nursing.job_category_name
            = align_by_h_job_family_catg_rn.job_category
        and cast(lookup_nursing_category.rn_job_ind as varchar(1))
            = align_by_h_job_family_catg_rn.rn_job_ind
),

set_h1_rows_rn_job_job_family as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_h1_rn_job_job_family.provider_alignment_use_ind,
        align_by_h1_rn_job_job_family.order_num,
        align_by_h1_rn_job_job_family.process_rank,
        align_by_h1_rn_job_job_family.align_by_h_job_group_id as job_group_id,
        align_by_h1_rn_job_job_family.root_job_hierarchy
    from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join {{ ref('lookup_nursing_category') }} as lookup_nursing_category
        on stg_job_profile_workday_plus_nursing.nursing_category = lookup_nursing_category.for_nursing_category
    inner join align_by_h1_rn_job_job_family
        on cast(lookup_nursing_category.rn_job_ind as varchar(1))
            = align_by_h1_rn_job_job_family.rn_job_ind
        and stg_job_profile_workday_plus_nursing.job_family = align_by_h1_rn_job_job_family.job_family
),

set_h2_rows_rn_job as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_h2_rn_job.provider_alignment_use_ind,
        align_by_h2_rn_job.order_num,
        align_by_h2_rn_job.process_rank,
        align_by_h2_rn_job.align_by_h_job_group_id as job_group_id,
        align_by_h2_rn_job.root_job_hierarchy
    from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join {{ ref('lookup_nursing_category') }} as lookup_nursing_category
        on stg_job_profile_workday_plus_nursing.nursing_category = lookup_nursing_category.for_nursing_category
    inner join align_by_h2_rn_job
        on cast(lookup_nursing_category.rn_job_ind as varchar(1))
            = align_by_h2_rn_job.rn_job_ind
)

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_g_rows_job_family_no_ncatg

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_h_rows_job_family_catg_rn

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_h1_rows_rn_job_job_family

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_h2_rows_rn_job
