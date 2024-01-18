{{ config(meta = {
    'critical': true
}) }}

with managers as (
        select
            manager_worker_wid,
            manager_name,
            count(*) as n_reports, -- this contains contingent workers
            case when count(*) > 0 then 1 else 0 end as is_manager_ind
        from
            {{ ref('stg_worker_job_position_history') }}
        where
            most_recent_primary_job_ind = 1
            and employment_end_date is null -- still active
        group by
            manager_worker_wid,
            manager_name
),

clarity_keys as (--49,356
        select
            stg_worker_employment_history.ad_login,
            employee.emp_key as clarity_emp_key,
            employee.prov_key,
            count(*) over(partition by stg_worker_employment_history.ad_login),
            row_number() over(
                partition by stg_worker_employment_history.ad_login
                order by employee.prov_key desc, employee.emp_id desc
            ) as priority_seq_num
        from
            {{ ref('stg_worker_employment_history') }} as stg_worker_employment_history
            inner join {{ source('cdw', 'employee') }} as employee
                on lower(employee.ad_login) = stg_worker_employment_history.ad_login
        where
            stg_worker_employment_history.most_recent_ind = 1
            and employee.comp_key = 1
),

reporting_chain as (
    select
        worker_contact_list.wd_worker_id as worker_id,
        worker_contact_list.reporting_chain_last_nms as reporting_chain
    from
        {{ source('workday', 'worker_contact_list') }} as worker_contact_list
    group by
        worker_contact_list.wd_worker_id,
        worker_contact_list.reporting_chain_last_nms
)

select
    --employment
    stg_worker_employment_history.worker_id,
    stg_worker_employment_history.legal_reporting_name,
    stg_worker_employment_history.preferred_reporting_name,
    stg_worker_employment_history.preferred_first_name,
    stg_worker_employment_history.display_name,
    stg_worker_employment_history.display_name_formatted,
    lower(stg_worker_employment_history.ad_login) as ad_login,
    stg_worker_employment_history.employee_ind,
    stg_worker_employment_history.active_ind,
    stg_worker_employment_history.current_tenure_years,
    stg_worker_employment_history.total_years_as_employee,
    stg_worker_employment_history.hire_date,
    stg_worker_employment_history.original_hire_date,
    stg_worker_employment_history.continuous_service_date,
    stg_worker_employment_history.seniority_date,
    stg_worker_employment_history.time_off_service_date,
    stg_worker_employment_history.termination_date,
    --latest position
    stg_worker_job_position_history.position_id,
    stg_worker_job_position_history.position_title,
    stg_worker_job_position_history.job_profile_id,
    stg_worker_job_position_history.job_code,
    stg_worker_job_position_history.job_title,
    stg_worker_job_position_history.job_title_display,
    stg_worker_job_position_history.job_classification,
    stg_worker_job_position_history.job_category,
    stg_worker_job_position_history.rn_job_ind,
    stg_worker_job_position_history.nccs_direct_care_staff_ind,
    stg_worker_job_position_history.magnet_reporting_name,
    stg_worker_job_position_history.magnet_reporting_ind,
    stg_worker_job_position_history.nursing_category,
    stg_worker_job_position_history.job_family_id,
    stg_worker_job_position_history.job_family,
    stg_worker_job_position_history.job_family_group_id,
    stg_worker_job_position_history.job_family_group,
    stg_worker_job_position_history.worker_type_id,
    stg_worker_job_position_history.worker_type,
    stg_worker_job_position_history.position_time_type,
    case
        when lower(stg_worker_job_position_history.position_time_type) = 'full_time' then 1 else 0
    end as full_time_ind,
    stg_worker_job_position_history.worker_role,
    stg_worker_job_position_history.management_level,
    stg_worker_job_position_history.location_name,
    stg_worker_job_position_history.location_hierarchy,
    stg_worker_job_position_history.cost_center_name,
    stg_worker_job_position_history.cost_center_site_name,
    stg_worker_job_position_history.manager_name,
    stg_worker_job_position_history.mgr_key,
    reporting_chain.reporting_chain,
    stg_worker_job_position_history.pay_group,
    stg_worker_job_position_history.ledger_code,
    stg_worker_job_position_history.fte_percentage,
    stg_worker_job_position_history.job_exempt_ind,
    stg_worker_job_position_history.scheduled_weekly_hours,
    stg_worker_job_position_history.pay_rate_type,
    --direct reports, suggest make a manager specific table to get breakdown of report types
    coalesce(managers.is_manager_ind, 0) as is_manager_ind,
    managers.n_reports,
    --ids
    employee.emp_key as workday_emp_key,
    clarity_keys.clarity_emp_key,
    clarity_keys.prov_key,
    stg_worker_employment_history.worker_wid,
    stg_worker_job_position_history.position_wid,
    stg_worker_job_position_history.job_family_wid,
    stg_worker_job_position_history.job_family_group_wid,
    stg_worker_job_position_history.job_profile_wid,
    stg_worker_job_position_history.location_wid,
    stg_worker_job_position_history.management_level_wid,
    stg_worker_job_position_history.manager_worker_wid,
    stg_worker_job_position_history.manager_id,
    stg_worker_job_position_history.cost_center_id,
    stg_worker_job_position_history.cost_center_site_id,
    stg_worker_job_position_history.worker_position_id as latest_worker_position_id,
    stg_worker_job_position_history.position_wid as latest_position_wid
from
    {{ ref('stg_worker_employment_history') }} as stg_worker_employment_history
    inner join {{ ref('stg_worker_job_position_history') }} as stg_worker_job_position_history
        on stg_worker_employment_history.worker_wid = stg_worker_job_position_history.worker_wid
    left join managers
        on managers.manager_worker_wid = stg_worker_employment_history.worker_wid
    left join clarity_keys
        on clarity_keys.ad_login = stg_worker_employment_history.ad_login
        and clarity_keys.priority_seq_num = 1
    left join {{ source('cdw', 'employee') }} as employee
        on employee.wd_worker_id = stg_worker_job_position_history.worker_id
    left join reporting_chain
        on reporting_chain.worker_id = stg_worker_employment_history.worker_id
where
    stg_worker_employment_history.most_recent_ind = 1
    and stg_worker_job_position_history.most_recent_primary_job_ind = 1
