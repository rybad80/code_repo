{{ config(meta = {
    'critical': true
}) }}

-- stg_job_group_s3_nursing_category_plus 
-- 23 i nursing_category magnet_reporting
-- 25 j nursing_category job_category
-- 28 k nursing_category

with align_by_i_ncatg_and_mag_report as (
    select
        job_group_id as align_by_i_job_group_id,
        attribute_1_value as nursing_category,
        attribute_2_value as magnet_reporting_name,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where attribute_1 = 'nursing_category'
        and attribute_2 = 'magnet_reporting_name'
        and attribute_3 is null
),

align_by_j_ncatg_and_jcatg as (
    select
        job_group_id as align_by_j_job_group_id,
        attribute_1_value as nursing_category,
        attribute_2_value as job_category,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where
        attribute_1 = 'nursing_category'
        and attribute_2 = 'job_category'
        and attribute_3 is null
),

align_by_k_ncatg as (
    select
        job_group_id as align_by_k_job_group_id,
        attribute_1_value as nursing_category,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ref('stg_job_group_s1_attribute_alignment_map')}}
    where
        attribute_1 = 'nursing_category'
        and attribute_2 is null
        and attribute_3 is null
),

set_i_rows_ncatg_and_mag_report as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_i_ncatg_and_mag_report.provider_alignment_use_ind,
        align_by_i_ncatg_and_mag_report.order_num,
        align_by_i_ncatg_and_mag_report.process_rank,
        align_by_i_ncatg_and_mag_report.align_by_i_job_group_id as job_group_id,
        align_by_i_ncatg_and_mag_report.root_job_hierarchy
 from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join align_by_i_ncatg_and_mag_report
        on stg_job_profile_workday_plus_nursing.nursing_category
            = align_by_i_ncatg_and_mag_report.nursing_category
        and stg_job_profile_workday_plus_nursing.magnet_reporting_name
            = align_by_i_ncatg_and_mag_report.magnet_reporting_name
),

set_j_rows_ncatg_and_jcatg as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_j_ncatg_and_jcatg.provider_alignment_use_ind,
        align_by_j_ncatg_and_jcatg.order_num,
        align_by_j_ncatg_and_jcatg.process_rank,
        align_by_j_ncatg_and_jcatg.align_by_j_job_group_id as job_group_id,
        align_by_j_ncatg_and_jcatg.root_job_hierarchy
    from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
    inner join align_by_j_ncatg_and_jcatg
        on stg_job_profile_workday_plus_nursing.nursing_category = align_by_j_ncatg_and_jcatg.nursing_category
        and stg_job_profile_workday_plus_nursing.job_category_name
            = align_by_j_ncatg_and_jcatg.job_category
),

set_k_rows_ncatg as (
    select
        stg_job_profile_workday_plus_nursing.job_code,
        align_by_k_ncatg.provider_alignment_use_ind,
        align_by_k_ncatg.order_num,
        align_by_k_ncatg.process_rank,
        align_by_k_ncatg.align_by_k_job_group_id as job_group_id,
        align_by_k_ncatg.root_job_hierarchy
  from {{ref('stg_job_profile_workday_plus_nursing')}} as stg_job_profile_workday_plus_nursing
   inner join align_by_k_ncatg
        on stg_job_profile_workday_plus_nursing.nursing_category = align_by_k_ncatg.nursing_category
)

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_i_rows_ncatg_and_mag_report

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_j_rows_ncatg_and_jcatg

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_k_rows_ncatg
