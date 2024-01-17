{{ config(meta = {
    'critical': true
}) }}

select
    stg_worker_employment_history.worker_id,
    worker_ods.birth_date::date as birth_date,
    worker_ods.death_date::date as death_date,
    substr(worker_ods.gender_code, 1, 1) as gender,
    worker_ods.hispanic_or_latino_ind,
    worker_ods.ethnicity_id,
    round(
            (
                coalesce(worker_employment.termination_date::date, current_date) - worker_ods.birth_date::date
            ) / 365.25,
            1
        ) as latest_age,
    stg_worker_employment_history.total_years_as_employee,
    stg_worker_employment_history.active_ind,
    stg_worker_employment_history.employee_ind,
    worker_employment.continuous_service_date::date as continuous_service_date,
    stg_worker_employment_history.termination_date::date as termination_date,
    worker_employment.termination_involuntary_ind,
    worker_employment.regrettable_termination_ind,
    worker_employment.rehire_ind,
    --keys
    stg_worker_employment_history.worker_wid
from
    {{ ref('stg_worker_employment_history') }} as stg_worker_employment_history
    inner join {{ source('workday_ods', 'worker') }} as worker_ods
        on worker_ods.worker_wid = stg_worker_employment_history.worker_wid
    inner join {{ source('workday_ods', 'worker_employment') }} as worker_employment
        on worker_employment.worker_wid = worker_ods.worker_wid
    inner join {{ ref('worker') }} as current_worker
        on current_worker.worker_wid = stg_worker_employment_history.worker_wid
