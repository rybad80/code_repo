{{ config(meta = {
    'critical': true
}) }}

with
    job_positions as (
        select distinct 
            position_wid,
            position_id,
            job_posting_title,
            job_profile_wid,
            job_profile_id,
            location_wid,
            location_id,
            position_time_type_id as position_time_type,
            worker_type_id,
            case
                when worker_type_id = 'CW' then 'Contingent Worker'
                else 'Employee'
                end as worker_role,
            coalesce(employee_type_id, contingent_worker_type_id) as worker_type
        from
            {{ source('workday_ods', 'job_position') }}
    ),
    
    job_org as (
        select distinct
            --about person
            worker_organization.worker_wid,
            worker_organization.user_id,
            worker_organization.pay_group_id as pay_group,
            worker_organization.glcode_id as ledger_code,
            -- about the supervisory department
            supervisory_department.supervisory_department_id as manager_id,
            supervisory_department.manager_worker_wid,
            manager.mgr_key,
            manager.emp_key as mgr_emp_key,
            worker.legal_reporting_name as manager_name,
            -- about the location or cost center
            worker_organization.cost_center_id,
            cost_center.cost_center_name,
            worker_organization.cost_center_site_id,
            cost_center_site.cost_center_site_name,
            worker_organization.location_hierarchy_id as location_hierarchy
        from
            {{ source('workday_ods', 'worker_organization') }} as worker_organization
            -- these will all need to use *_wid instead of *_id in their joins, these are currently all 1:1 from the API
            left join {{ source('workday_ods', 'supervisory_department') }} as supervisory_department
                on supervisory_department.supervisory_department_id = worker_organization.department_id
            left join {{ source('workday_ods', 'cost_center') }} as cost_center
                on cost_center.cost_center_id = worker_organization.cost_center_id
            left join {{ source('workday_ods', 'cost_center_site') }} as cost_center_site
                on cost_center_site.cost_center_site_id = worker_organization.cost_center_site_id
            left join {{ source('cdw', 'manager') }} as manager
                on manager.mgr_id = worker_organization.department_id
                and worker_organization.department_id != '2000_CT'
            left join {{ source('workday_ods', 'worker') }} as worker
                on worker.worker_wid = supervisory_department.manager_worker_wid
    )

select
    {{
        dbt_utils.surrogate_key([
            'worker_job.position_wid',
            'worker_job.worker_wid'
        ])
    }} as worker_position_id,
    worker_job.worker_wid,
    worker_job.worker_id,
    lower(worker_job.user_id) as ad_login,
    worker.legal_reporting_name as legal_reporting_name,
    worker.preferred_reporting_name as preferred_reporting_name,
    -- find order of positions by date and primary_ind
    row_number() over (
            partition by worker_job.worker_wid
            order by
                worker_job.effective_date,
                worker_job.start_date,
                worker_job.create_dt
        ) as job_position_seq_num,
    case
        when row_number() over (
            partition by worker_job.worker_wid
            order by
                worker_job.primary_job_ind desc,
                worker_job.effective_date desc,
                worker_job.start_date desc,
                worker_job.create_dt desc
            ) = 1
            then 1
        else 0
        end as most_recent_primary_job_ind,
    -- dates
    worker_job.effective_date as position_start_date,
    count(*) over(partition by worker_job.worker_wid, worker_job.effective_date) as n_position_on_date,
    case
        when
            lead(
                worker_job.effective_date - 1
            ) over(
                partition by worker_job.worker_wid order by worker_job.effective_date
            ) > worker_job.effective_date
        then lead(
            worker_job.effective_date - 1
        ) over(partition by worker_job.worker_wid order by worker_job.effective_date)
        else worker_job.end_date
        end as position_end_date,
    worker_job.start_date as employment_start_date,
    worker_job.end_date as employment_end_date,
    -- role
    worker_job.position_title,
    job_positions.position_id,
    job_positions.job_posting_title,
    job_profile.job_code,
    job_profile.job_title,
    job_profile.job_code || ' - ' || job_profile.job_title as job_title_display,
    job_profile_report.job_classifications as job_classification,
    job_profile_report.job_category as job_category,
    case 
        when job_profile_report.magnetreporting is null then null
        when job_profile_report.magnetreporting = 'Peer Review Exception' then null
        when job_profile_report.magnetreporting in (
            'Licensed Practical/Vocational Nurse',
            'Nurse Managers',
            'Unlicensed Assistive Personnel'
            ) then job_profile_report.magnetreporting
        else coalesce(lookup_worker_nursing_category.nursing_category_group, 'Systems Support Nurse')
        end as nursing_category,
    case
        when job_profile_report.magnetreporting = 'Peer Review Exception' then null
        when job_profile_report.magnetreporting is not null then job_profile_report.magnetreporting
    end as magnet_reporting_name,
    case when length(magnet_reporting_name) > 1 then 1 else 0 end as magnet_reporting_ind,
    case
        --when magnet_reporting_ind = 0 then 0
        when nursing_category in (
            'Acute Care Nurse',
            'Advanced Practice Provider',
            'Ambulatory Nurse',
            'Nurse Managers',
            'Director and Executive',
            'Systems Support Nurse'
        ) then 1 else 0 
    end as rn_job_ind,
    
    case
        --when magnet_reporting_ind = 0 then 0
        when nursing_category in (
            'Acute Care Nurse',
            'Advanced Practice Provider',
            'Ambulatory Nurse',
            'Licensed Practical/Vocational Nurse',
            'Unlicensed Assistive Personnel'
        ) then 1 else 0 
    end as nccs_direct_care_staff_ind,
    job_family_group.job_family_group_id,
    job_family_group.name as job_family_group,
    job_family.job_family_id,
    job_family.name as job_family,
    coalesce(worker_job.job_profile_id, job_positions.job_profile_id) as job_profile_id,
    coalesce(
        case when worker_job.employee_type_id is null then 'CW' else 'EE' end,
        job_positions.worker_type_id
        ) as worker_type_id,
    coalesce(
            case when worker_job.employee_type_id is null then 'Contingent Worker' else 'Employee' end,
            job_positions.worker_type
        ) as worker_type,
    coalesce(worker_job.position_time_type_profile_id, job_positions.position_time_type) as position_time_type,
    coalesce(
        worker_job.employee_type_id, worker_job.contingent_worker_type_id, job_positions.worker_type
    ) as worker_role,
    job_profile.management_level_id as management_level,
    -- where
    job_org.manager_id,
    job_org.manager_name,
    location.location_id,
    location.location_name,
    job_org.location_hierarchy,
    job_org.cost_center_id,
    job_org.cost_center_name,
    job_org.cost_center_site_id,
    job_org.cost_center_site_name,
    -- pay 
    job_org.pay_group,
    job_org.ledger_code,
    worker_job.full_time_equivalent_percentage as fte_percentage,
    worker_job.job_exempt_ind,
    worker_job.scheduled_weekly_hours,
    job_profile.pay_rate_type_id as pay_rate_type,
    -- keys
    worker_job.position_wid,
    job_profile.job_family_wid,
    job_family.job_family_group_wid,
    job_profile.job_profile_wid,
    location.location_wid,
    job_profile.management_level_wid,
    job_org.mgr_key,
    job_org.mgr_emp_key,
    job_org.manager_worker_wid
from
    {{ source('workday_ods', 'worker_job') }} as worker_job
    left join {{ source('workday_ods', 'worker') }} as worker 
        on worker.worker_wid = worker_job.worker_wid
    left join job_positions
        on job_positions.position_wid = worker_job.position_wid
    left join job_org
        on job_org.worker_wid = worker_job.worker_wid
    left join {{ source('workday_ods', 'job_profile_report') }} as job_profile_report 
        on job_profile_report.job_profile_wid = coalesce(
                worker_job.job_profile_wid, 
                job_positions.job_profile_wid
            )
    left join {{ source('workday_ods', 'job_profile') }} as job_profile 
        on job_profile.job_profile_wid = coalesce(
                worker_job.job_profile_wid, 
                job_positions.job_profile_wid
            )
    left join {{ source('workday_ods', 'job_family') }} as job_family 
        on job_family.job_family_wid = job_profile.job_family_wid
    left join {{ source('workday_ods', 'job_family_group') }} as job_family_group 
    on job_family_group.job_family_group_wid = job_family.job_family_group_wid    
    left join  {{ ref('lookup_worker_nursing_category') }} as lookup_worker_nursing_category 
        on lookup_worker_nursing_category.nursing_job_family = job_family.name
    left join {{ source('workday_ods', 'location') }} as location
        on location.location_wid = coalesce(
            worker_job.business_site_location_wid, 
            job_positions.location_wid
        )
where
    worker_job.effective_date::date <= current_date
