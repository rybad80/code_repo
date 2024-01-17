{{ config(meta = {
    'critical': true
}) }}

/* stg_job_group_s4_magnet_reporting_plus
    30 l magnet_reporting management_level
    33 m magnet_reporting job_family (20=no magnet value + job family)
    35 n magnet_reporting
*/
with align_by_l_mag_report_and_mgmt_lvl as (
    select
        job_group_id as align_by_l_job_group_id,
        attribute_1_value as magnet_reporting_name,
        attribute_2_value as management_level,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ ref('stg_job_group_s1_attribute_alignment_map') }}
    where attribute_1 = 'magnet_reporting_name'
        and attribute_2 = 'management_level'
        and attribute_3 is null
),

align_by_m_mag_report_and_job_fam as (
    select
        job_group_id as align_by_m_job_group_id,
        attribute_1_value as magnet_reporting_name,
        attribute_2_value as job_family,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ ref('stg_job_group_s1_attribute_alignment_map') }}
    where
        (attribute_1 = 'magnet_reporting_name'
        and attribute_2 = 'job_family'
        and attribute_3 is null)
        or /* NULL nursing category is same as null magenet_reporting, needed for PAs */
        (attribute_1 = 'nursing_category'
        and attribute_1_value = 'NULL'
        and attribute_2 = 'job_family'
        and attribute_3 is null)
),

align_by_n_mag_report as (
    select
        job_group_id as align_by_n_job_group_id,
        attribute_1_value as magnet_reporting_name,
        root_job_hierarchy,
        provider_alignment_use_ind,
        order_num,
        process_rank
    from {{ ref('stg_job_group_s1_attribute_alignment_map') }}
    where
        attribute_1 = 'magnet_reporting_name'
        and attribute_2 is null
        and attribute_3 is null
),

set_l_rows_mag_report_and_mgmt_lvl as (
    select
        job_align_by_lvl.job_code,
        align_by_l_mag_report_and_mgmt_lvl.provider_alignment_use_ind,
        align_by_l_mag_report_and_mgmt_lvl.order_num,
        align_by_l_mag_report_and_mgmt_lvl.process_rank,
        align_by_l_mag_report_and_mgmt_lvl.align_by_l_job_group_id as job_group_id,
        align_by_l_mag_report_and_mgmt_lvl.root_job_hierarchy
 from {{ ref('stg_job_profile_workday_plus_nursing') }} as job_align_by_lvl
    inner join align_by_l_mag_report_and_mgmt_lvl
        on job_align_by_lvl.magnet_reporting_name
            = align_by_l_mag_report_and_mgmt_lvl.magnet_reporting_name
        and job_align_by_lvl.management_level
            = align_by_l_mag_report_and_mgmt_lvl.management_level
),

set_m_rows_mag_report_and_job_fam as (
    select
        job_align_by_family.job_code,
        align_by_m_mag_report_and_job_fam.provider_alignment_use_ind,
        align_by_m_mag_report_and_job_fam.order_num,
        align_by_m_mag_report_and_job_fam.process_rank,
        align_by_m_mag_report_and_job_fam.align_by_m_job_group_id as job_group_id,
        align_by_m_mag_report_and_job_fam.root_job_hierarchy
    from {{ ref('stg_job_profile_workday_plus_nursing') }} as job_align_by_family
    inner join align_by_m_mag_report_and_job_fam
        on coalesce(job_align_by_family.magnet_reporting_name, 'NULL')
            = align_by_m_mag_report_and_job_fam.magnet_reporting_name
        and job_align_by_family.job_family
            = align_by_m_mag_report_and_job_fam.job_family
),

set_n_rows_mag_report as (
    select
        job_align_by_magnet.job_code,
        align_by_n_mag_report.provider_alignment_use_ind,
        align_by_n_mag_report.order_num,
        align_by_n_mag_report.process_rank,
        align_by_n_mag_report.align_by_n_job_group_id as job_group_id,
        align_by_n_mag_report.root_job_hierarchy
  from {{ ref('stg_job_profile_workday_plus_nursing') }} as job_align_by_magnet
   inner join align_by_n_mag_report
        on job_align_by_magnet.magnet_reporting_name = align_by_n_mag_report.magnet_reporting_name
)

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_l_rows_mag_report_and_mgmt_lvl

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_m_rows_mag_report_and_job_fam

union all

select
    job_code,
    provider_alignment_use_ind,
    order_num,
    process_rank,
    job_group_id,
    root_job_hierarchy
from set_n_rows_mag_report
