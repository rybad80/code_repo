{{ config(meta = {
    'critical': true
}) }}

select
    workday_plus_nursing.job_code,
    workday_plus_nursing.job_title_display,
    profile_provider_alignment.provider_job_group_id,
    profile_provider_alignment.rn_alt_job_group_id,
    workday_plus_nursing.nursing_category,
    workday_plus_nursing.job_category_name,
    workday_plus_nursing.job_category_sort_num,
    workday_plus_nursing.job_classification_name,
    workday_plus_nursing.job_classification_sort_num,
    workday_plus_nursing.job_family,
    workday_plus_nursing.job_family_group,
    workday_plus_nursing.job_family_group_id,
    workday_plus_nursing.job_family_group_wid,
    workday_plus_nursing.job_family_id,
    workday_plus_nursing.job_family_wid,
    workday_plus_nursing.job_profile_wid,
    workday_plus_nursing.job_title,
    workday_plus_nursing.magnet_reporting_ind,
    workday_plus_nursing.magnet_reporting_name,
    workday_plus_nursing.management_level,
    workday_plus_nursing.management_level_wid,
    lookup_nursing_category.nccs_direct_care_staff_ind,
    lookup_nursing_category.nursing_category_abbreviation,
    lookup_nursing_category.nursing_category_sort_num,
    workday_plus_nursing.pay_rate_type,
    coalesce(lookup_nursing_category.rn_job_ind, 0) as rn_job_ind,
    lookup_nursing_category.bedside_rn_ind,
    job_profile_report.careandcontacttype as care_and_contact_type,
    case job_profile_report.careandcontacttype
        when 'Direct' then 1
        when 'Indirect' then 1
        else 0
    end
    as healthcare_worker_job_ind,
    job_profile_report.oshacategory as osha_category,
    job_profile_report.jobclass as payroll_job_class
from {{ ref('stg_job_profile_provider_alignment') }} as profile_provider_alignment
left join {{ ref('lookup_nursing_category') }} as lookup_nursing_category
    on profile_provider_alignment.nursing_category = lookup_nursing_category.for_nursing_category
left join {{ source('workday_ods', 'job_profile_report') }}  as job_profile_report
    on profile_provider_alignment.job_code = job_profile_report.job_code
inner join {{ ref('stg_job_profile_workday_plus_nursing') }} as workday_plus_nursing
    on workday_plus_nursing.job_code = profile_provider_alignment.job_code
