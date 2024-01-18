{{
    config(
        materialized = 'incremental',
        unique_key = 'worker_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['worker_wid', 'worker_id', 'user_id', 'universal_id', 'employee_id', 'contingent_worker_id', 'active_ind', 'active_status_date', 'hire_date', 'original_hire_date', 'continuous_service_date', 'first_day_of_work_date', 'retired_ind', 'seniority_date', 'time_off_service_date', 'days_unemployed', 'months_continuous_prior_employment', 'terminated_ind', 'termination_date', 'pay_through_date', 'regrettable_termination_ind', 'hire_rescinded_ind', 'termination_last_day_of_work_date', 'resignation_date', 'not_returning_ind', 'return_unknown_ind', 'termination_involuntary_ind', 'not_eligible_for_hire_ind', 'not_eligible_for_rehire_ind', 'rehire_ind', 'end_employment_date', 'expected_retirement_date', 'retirement_eligibility_date', 'retirement_date', 'severance_date', 'benefits_service_date', 'company_service_date', 'vesting_date', 'date_entered_workforce_date', 'last_date_for_which_paid_date', 'expected_date_of_return_date', 'probation_start_date', 'probation_end_date', 'academic_tenure_date', 'md5', 'upd_dt', 'upd_by']
    )
}}
with worker_employment_data as (
    select distinct
        get_workers_employment_data.worker_worker_reference_wid as worker_wid,
        get_workers_employment_data.worker_worker_data_worker_id as worker_id,
        get_workers_employment_data.worker_worker_data_user_id as user_id,
        get_workers_employment_data.worker_worker_data_universal_id as universal_id,
        cast(cast(get_workers_employment_data.worker_worker_reference_employee_id as int) as varchar(50)) as employee_id,
        cast(cast(get_workers_employment_data.worker_worker_reference_contingent_worker_id as int) as varchar(50)) as contingent_worker_id,
        coalesce(cast(get_workers_employment_data.employment_data_worker_status_data_active as int), -2) as active_ind,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_active_status_date, 'yyyy-mm-dd') as active_status_date,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_hire_date, 'yyyy-mm-dd') as hire_date,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_original_hire_date, 'yyyy-mm-dd') as original_hire_date,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_continuous_service_date, 'yyyy-mm-dd') as continuous_service_date,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_first_day_of_work, 'yyyy-mm-dd') as first_day_of_work_date,
        coalesce(cast(get_workers_employment_data.employment_data_worker_status_data_retired as int), -2) as retired_ind,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_seniority_date, 'yyyy-mm-dd') as seniority_date,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_time_off_service_date, 'yyyy-mm-dd') as time_off_service_date,
        cast(get_workers_employment_data.employment_data_worker_status_data_days_unemployed as numeric(30,2)) as days_unemployed,
        cast(get_workers_employment_data.employment_data_worker_status_data_months_continuous_prior_employment as numeric(30,2)) as months_continuous_prior_employment,
        coalesce(cast(get_workers_employment_data.employment_data_worker_status_data_terminated as int), -2) as terminated_ind,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_termination_date, 'yyyy-mm-dd') as termination_date,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_pay_through_date, 'yyyy-mm-dd') as pay_through_date,
        coalesce(cast(get_workers_employment_data.employment_data_worker_status_data_regrettable_termination as int), -2) as regrettable_termination_ind,
        coalesce(cast(get_workers_employment_data.employment_data_worker_status_data_hire_rescinded as int), -2) as hire_rescinded_ind,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_termination_last_day_of_work, 'yyyy-mm-dd') as termination_last_day_of_work_date,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_resignation_date, 'yyyy-mm-dd') as resignation_date,
        coalesce(cast(get_workers_employment_data.employment_data_worker_status_data_not_returning as int), -2) as not_returning_ind,
        coalesce(cast(get_workers_employment_data.employment_data_worker_status_data_return_unknown as int), -2) as return_unknown_ind,
        coalesce(cast(get_workers_employment_data.employment_data_worker_status_data_termination_involuntary as int), -2) as termination_involuntary_ind,
        coalesce(case when get_workers_employment_data.worker_status_data_eligible_for_hire_reference_yes_no_type_id = 'Yes' then 1 when get_workers_employment_data.worker_status_data_eligible_for_hire_reference_yes_no_type_id = 'No' then 0 end, -2) as not_eligible_for_hire_ind,
        coalesce(case when get_workers_employment_data.worker_status_data_eligible_for_rehire_on_latest_termination_reference_yes_no_type_id = 'Yes' then 1 when get_workers_employment_data.worker_status_data_eligible_for_rehire_on_latest_termination_reference_yes_no_type_id = 'No' then 0 end, -2) as not_eligible_for_rehire_ind,
        coalesce(cast(get_workers_employment_data.employment_data_worker_status_data_rehire as int), -2) as rehire_ind,
        to_timestamp(get_workers_employment_data.worker_job_data_position_data_end_date, 'yyyy-mm-dd') as end_employment_date,
        cast(null as timestamp) as expected_retirement_date,
        cast(null as timestamp) as retirement_eligibility_date,
        cast(null as timestamp) as retirement_date,
        cast(null as timestamp) as severance_date,
        cast(null as timestamp) as benefits_service_date,
        to_timestamp(get_workers_employment_data.employment_data_worker_status_data_company_service_date, 'yyyy-mm-dd') as company_service_date,
        cast(null as timestamp) as vesting_date,
        cast(null as timestamp) as date_entered_workforce_date,
        cast(null as timestamp) as last_date_for_which_paid_date,
        cast(null as timestamp) as expected_date_of_return_date,
        cast(null as timestamp) as probation_start_date,
        cast(null as timestamp) as probation_end_date,
        cast(null as timestamp) as academic_tenure_date,
        cast({{
            dbt_utils.surrogate_key([
                'worker_wid',
                'worker_id',
                'user_id',
                'universal_id',
                'employee_id',
                'contingent_worker_id',
                'active_ind',
                'active_status_date',
                'hire_date',
                'original_hire_date',
                'continuous_service_date',
                'first_day_of_work_date',
                'retired_ind',
                'seniority_date',
                'time_off_service_date',
                'days_unemployed',
                'months_continuous_prior_employment',
                'terminated_ind',
                'termination_date',
                'pay_through_date',
                'regrettable_termination_ind',
                'hire_rescinded_ind',
                'termination_last_day_of_work_date',
                'resignation_date',
                'not_returning_ind',
                'return_unknown_ind',
                'termination_involuntary_ind',
                'not_eligible_for_hire_ind',
                'not_eligible_for_rehire_ind',
                'rehire_ind',
                'end_employment_date',
                'expected_retirement_date',
                'retirement_eligibility_date',
                'retirement_date',
                'severance_date',
                'benefits_service_date',
                'company_service_date',
                'vesting_date',
                'date_entered_workforce_date',
                'last_date_for_which_paid_date',
                'expected_date_of_return_date',
                'probation_start_date',
                'probation_end_date',
                'academic_tenure_date',
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_workers_employment_data')}} as get_workers_employment_data
)
select
    worker_wid,
    worker_id,
    user_id,
    universal_id,
    employee_id,
    contingent_worker_id,
    active_ind,
    active_status_date,
    hire_date,
    original_hire_date,
    continuous_service_date,
    first_day_of_work_date,
    retired_ind,
    seniority_date,
    time_off_service_date,
    days_unemployed,
    months_continuous_prior_employment,
    terminated_ind,
    termination_date,
    pay_through_date,
    regrettable_termination_ind,
    hire_rescinded_ind,
    termination_last_day_of_work_date,
    resignation_date,
    not_returning_ind,
    return_unknown_ind,
    termination_involuntary_ind,
    not_eligible_for_hire_ind,
    not_eligible_for_rehire_ind,
    rehire_ind,
    end_employment_date,
    expected_retirement_date,
    retirement_eligibility_date,
    retirement_date,
    severance_date,
    benefits_service_date,
    company_service_date,
    vesting_date,
    date_entered_workforce_date,
    last_date_for_which_paid_date,
    expected_date_of_return_date,
    probation_start_date,
    probation_end_date,
    academic_tenure_date,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    worker_employment_data
where
    1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where worker_wid = worker_employment_data.worker_wid)
    {%- endif %}
