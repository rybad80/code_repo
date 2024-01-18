{{ config(meta = {
    'critical': true
}) }}

with job_profile_report as (
    select
        job_profile_wid,
        replace(job_classifications, ' - (EEO Category-United States of America)', '') as job_classification,
        instr(job_classification, ' ' ) as space_after_class_num,
        job_category,
        instr(job_category, '-' ) as dash_after_category_num,
        substr(job_classification, 1, space_after_class_num - 1)::numeric as job_classification_sort_num,
        substr(job_classification, space_after_class_num + 1) as job_classification_name,
        substr(job_category, 1, dash_after_category_num - 2)::numeric as job_category_sort_num,
        substr(job_category, dash_after_category_num + 2) as job_category_name,
        magnetreporting
    from {{source('workday_ods', 'job_profile_report')}}
)

select
    job_profile.job_code,
    job_profile.job_title,
    job_profile.job_code || ' - ' || job_profile.job_title as job_title_display,
    job_profile_report.job_classification_name,
    job_profile_report.job_category_name,
    case when job_profile_report.magnetreporting is null then null
        when job_profile_report.magnetreporting = 'Peer Review Exception' then null
        when job_profile_report.magnetreporting = 'Nurse Managers' then 'Supervisor and Manager'
        when
            job_profile_report.magnetreporting in (
                'Licensed Practical/Vocational Nurse', 'Unlicensed Assistive Personnel'
            )
            then job_profile_report.magnetreporting
        else coalesce(lookup_job_family_nursing_category.nursing_category, 'Systems Support Nurse')
    end as nursing_category,
    case when job_profile_report.magnetreporting = 'Peer Review Exception' then null
            when job_profile_report.magnetreporting is not null then job_profile_report.magnetreporting
    end as magnet_reporting_name,
    case when length(magnet_reporting_name) > 1 then 1 else 0 end as magnet_reporting_ind,
    job_family_group.job_family_group_id,
    job_family_group.name as job_family_group,
    job_family.job_family_id,
    job_family.name as job_family,
    job_profile.management_level_id as management_level,
    job_profile.pay_rate_type_id as pay_rate_type,
-- keys
    job_profile.job_family_wid,
    job_family.job_family_group_wid,
    job_profile.job_profile_wid,
    job_profile.management_level_wid,
    job_profile_report.job_classification_sort_num,
    job_profile_report.job_category_sort_num
from {{source('workday_ods', 'job_profile')}} as job_profile
left join job_profile_report
    on job_profile.job_profile_wid = job_profile_report.job_profile_wid
left join {{source('workday_ods', 'job_family')}} as job_family
    on job_family.job_family_wid = job_profile.job_family_wid
left join {{source('workday_ods', 'job_family_group')}} as job_family_group
    on job_family_group.job_family_group_wid = job_family.job_family_group_wid
left join {{ ref('lookup_job_family_nursing_category') }} as lookup_job_family_nursing_category
    on lookup_job_family_nursing_category.job_family = job_family.name
