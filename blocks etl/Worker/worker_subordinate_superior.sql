{{ config(meta = {
    'critical': true
}) }}

with management_level as (
    select
        management_level,
        management_level_sort_num,
        management_level_rank,
        management_level_abbr
    from
        {{ref('lookup_management_level')}}
),

superior_info as (
    select
        employee.legal_last_nm as superior_last_name,
        lower(employee.ad_login) as superior_ad_login,
        employee.emp_id as superior_worker_id,
        management_level.management_level_abbr as superior_management_level_abbr,
        management_level.management_level_sort_num as superior_management_level_sort_num,
        employee.emp_key as superior_emp_key
    from
        {{source('cdw', 'employee')}} as employee
    left join management_level
        on employee.management_level = management_level.management_level
),

subordinate_info as (
    select
        worker.worker_id as subordinate_worker_id,
        worker.ad_login as subordinate_ad_login,
        management_level.management_level_abbr as subordinate_management_level_abbr,
        management_level.management_level_sort_num as subordinate_management_level_sort_num,
        worker.active_ind as worker_active_ind,
        worker.employee_ind as worker_employee_ind,
        worker.active_ind * worker.rn_job_ind as worker_active_rn_job_ind,
        worker.workday_emp_key as worker_emp_key,
        worker_management_level.reporting_level as worker_reporting_level
    from
        {{ref('worker')}} as worker
    left join {{ref('stg_worker_management_level')}} as worker_management_level
        on worker.worker_id = worker_management_level.wd_worker_id
    left join management_level
        on worker.management_level = management_level.management_level
)

    select
        subordinate_worker_row.worker_id,
        subordinate_info.subordinate_ad_login,
        subordinate_info.subordinate_management_level_abbr,
        subordinate_info.subordinate_management_level_sort_num,
        subordinate_info.worker_active_ind,
        subordinate_info.worker_active_rn_job_ind,
        subordinate_info.worker_employee_ind,
        subordinate_info.worker_emp_key,
        subordinate_info.worker_reporting_level,
        subordinate_worker_row.superior_emp_key,
        subordinate_worker_row.direct_supervisor_ind,
        subordinate_worker_row.superior_lvl,
        superior_info.superior_last_name,
        superior_info.superior_worker_id,
        superior_info.superior_ad_login,
        superior_info.superior_management_level_abbr
    from {{ref('stg_subordinate_worker_row')}} as subordinate_worker_row
    left join superior_info
        on subordinate_worker_row.superior_emp_key = superior_info.superior_emp_key
    left join subordinate_info
        on subordinate_worker_row.worker_id = subordinate_info.subordinate_worker_id
