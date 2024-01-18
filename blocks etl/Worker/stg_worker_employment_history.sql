{{ config(meta = {
    'critical': true
}) }}

select
    worker_employment.worker_wid,
    worker_employment.worker_id,
    worker.legal_reporting_name,
    worker.preferred_reporting_name,
    case
        when (
            (lower(worker.preferred_first_name) = lower(worker.legal_first_name))
            or (lower(worker.preferred_first_name) = lower(worker.preferred_last_name))
            or (lower(worker.legal_first_name) like lower(worker.preferred_first_name) || ' %')
        ) then worker.legal_reporting_name
        else worker.legal_reporting_name || ' (' || worker.preferred_first_name || ')'
        end as display_name,
    case
        when (
            (lower(worker.preferred_first_name) = lower(worker.legal_first_name))
            or (lower(worker.preferred_first_name) = lower(worker.preferred_last_name))
            or (lower(worker.legal_first_name) like lower(worker.preferred_first_name) || ' %')
        ) then worker.legal_formated_name
        else worker.legal_first_name || ' (' || worker.preferred_first_name || ') ' || worker.legal_last_name
        end as display_name_formatted,
    worker.preferred_first_name,
    lower(worker_employment.user_id) as ad_login,
    row_number() over(
        partition by worker_employment.worker_id order by worker_employment.hire_date
    ) as hire_seq_num,
    case
        when
            row_number() over(
                partition by
                    worker_employment.worker_id
                order by
                    worker_employment.hire_date desc,
                    coalesce(worker_employment.termination_date, current_date) desc
            ) = 1 then 1
        else 0
        end as most_recent_ind,
    case
        when worker_employment.employee_id is not null then 'Employee'
        else 'Contingent Worker'
        end as worker_role,
    nvl2(worker_employment.employee_id, 1, 0) as employee_ind,
    worker_employment.active_ind,
    round(
            (
                coalesce(
                    worker_employment.termination_date::date, current_date
                ) - worker_employment.hire_date::date
            ) / 365.25,
            1
        ) as current_tenure_years,
    round(
            (
                coalesce(
                    worker_employment.termination_date::date, current_date
                ) - worker_employment.continuous_service_date::date
            ) / 365.25,
            1
        ) as total_years_as_employee,
    worker_employment.hire_date,
    worker_employment.original_hire_date,
    worker_employment.continuous_service_date,
    worker_employment.seniority_date,
    worker_employment.time_off_service_date,
    worker_employment.termination_date::date as termination_date
from
    {{ source('workday_ods', 'worker_employment') }} as worker_employment
    inner join {{ source('workday_ods', 'worker') }} as worker
        on worker.worker_wid = worker_employment.worker_wid
