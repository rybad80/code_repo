{{
    config(
        materialized = 'incremental',
        unique_key = 'worker_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['worker_wid', 'worker_id', 'user_id', 'universal_id', 'employee_id', 'contingent_worker_id', 'compensation_effective_date', 'emp_comp_summary_total_base_pay', 'emp_comp_summary_total_salary_and_allowances', 'emp_comp_summary_primary_compensation_basis', 'annualized_total_base_pay', 'annualized_total_salary_and_allowances', 'annualized_primary_compensation_basis', 'pay_group_frequency_total_base_pay', 'pay_group_frequency_total_salary_and_allowances', 'pay_group_frequency_primary_compensation_basis', 'annualized_reporting_total_base_pay', 'annualized_reporting_total_salary_and_allowances', 'annualized_reporting_primary_compensation_basis', 'hourly_frequency_total_base_pay', 'compensation_reason_wid', 'general_event_subcategory_id', 'event_classification_subcategory_id', 'change_job_subcategory_id', 'paygroup_frequency_id', 'paygroup_currency_id', 'annualized_currency_id', 'annualized_frequency_id', 'annualized_reporting_frequency_id', 'annualized_reporting_currency_id', 'hourly_frequency_id', 'hourly_frequency_currency_id', 'summary_frequency_id', 'summary_currency_id', 'md5', 'upd_dt', 'upd_by']
    )
}}
with worker_comp as (
select
distinct
    get_workers_compensation_data.worker_worker_reference_wid as worker_wid,
    get_workers_compensation_data.worker_worker_data_worker_id as worker_id,
    get_workers_compensation_data.worker_worker_data_user_id as user_id,
    get_workers_compensation_data.worker_worker_data_universal_id as universal_id,
    cast(cast(get_workers_compensation_data.worker_worker_reference_employee_id as int) as varchar(50)) as employee_id,
    cast(cast(get_workers_compensation_data.worker_worker_reference_contingent_worker_id as int) as varchar(50)) as contingent_worker_id,
    to_timestamp(get_workers_compensation_data.worker_data_compensation_data_compensation_effective_date, 'yyyy-mm-dd') as compensation_effective_date,
    cast(get_workers_compensation_data.employee_compensation_summary_data_employee_compensation_summary_data_total_base_pay as numeric(30,2)) as emp_comp_summary_total_base_pay,
    cast(get_workers_compensation_data.employee_compensation_summary_data_employee_compensation_summary_data_total_salary_and_allowances as numeric(30,2)) as emp_comp_summary_total_salary_and_allowances,
    cast(get_workers_compensation_data.employee_compensation_summary_data_employee_compensation_summary_data_primary_compensation_basis as numeric(30,2)) as emp_comp_summary_primary_compensation_basis,
    cast(get_workers_compensation_data.employee_compensation_summary_data_annualized_summary_data_total_base_pay as numeric(30,2)) as annualized_total_base_pay,
    cast(get_workers_compensation_data.employee_compensation_summary_data_annualized_summary_data_total_salary_and_allowances as numeric(30,2)) as annualized_total_salary_and_allowances,
    cast(get_workers_compensation_data.employee_compensation_summary_data_annualized_summary_data_primary_compensation_basis as numeric(30,2)) as annualized_primary_compensation_basis,
    cast(get_workers_compensation_data.employee_compensation_summary_data_summary_data_in_pay_group_frequency_total_base_pay as numeric(30,2)) as pay_group_frequency_total_base_pay,
    cast(get_workers_compensation_data.employee_compensation_summary_data_summary_data_in_pay_group_frequency_total_salary_and_allowances as numeric(30,2)) as pay_group_frequency_total_salary_and_allowances,
    cast(get_workers_compensation_data.employee_compensation_summary_data_summary_data_in_pay_group_frequency_primary_compensation_basis as numeric(30,2)) as pay_group_frequency_primary_compensation_basis,
    cast(get_workers_compensation_data.employee_compensation_summary_data_annualized_in_reporting_currency_summary_data_total_base_pay as numeric(30,2)) as annualized_reporting_total_base_pay,
    cast(get_workers_compensation_data.employee_compensation_summary_data_annualized_in_reporting_currency_summary_data_total_salary_and_allowances as numeric(30,2)) as annualized_reporting_total_salary_and_allowances,
    cast(get_workers_compensation_data.employee_compensation_summary_data_annualized_in_reporting_currency_summary_data_primary_compensation_basis as numeric(30,2)) as annualized_reporting_primary_compensation_basis,
    cast(get_workers_compensation_data.employee_compensation_summary_data_summary_data_in_hourly_frequency_total_base_pay as numeric(30,2)) as hourly_frequency_total_base_pay,
    get_workers_compensation_data.compensation_data_reason_reference_wid as compensation_reason_wid,
    get_workers_compensation_data.compensation_data_reason_reference_general_event_subcategory_id as general_event_subcategory_id,
    get_workers_compensation_data.compensation_data_reason_reference_event_classification_subcategory_id as event_classification_subcategory_id,
    get_workers_compensation_data.compensation_data_reason_reference_change_job_subcategory_id as change_job_subcategory_id,
    get_workers_compensation_data.summary_data_in_pay_group_frequency_frequency_reference_frequency_id as paygroup_frequency_id,
    get_workers_compensation_data.summary_data_in_pay_group_frequency_currency_reference_currency_id as paygroup_currency_id,
    get_workers_compensation_data.annualized_summary_data_currency_reference_currency_id as annualized_currency_id,
    get_workers_compensation_data.annualized_summary_data_frequency_reference_frequency_id as annualized_frequency_id,
    get_workers_compensation_data.annualized_in_reporting_currency_summary_data_frequency_reference_frequency_id as annualized_reporting_frequency_id,
    get_workers_compensation_data.annualized_in_reporting_currency_summary_data_currency_reference_currency_id as annualized_reporting_currency_id,
    get_workers_compensation_data.summary_data_in_hourly_frequency_frequency_reference_frequency_id as hourly_frequency_id,
    get_workers_compensation_data.summary_data_in_hourly_frequency_currency_reference_currency_id as hourly_frequency_currency_id,
    get_workers_compensation_data.employee_compensation_summary_data_frequency_reference_frequency_id as summary_frequency_id,
    get_workers_compensation_data.employee_compensation_summary_data_currency_reference_currency_id as summary_currency_id,
    cast({{
        dbt_utils.surrogate_key([
            'worker_wid',
            'worker_id',
            'user_id',
            'universal_id',
            'employee_id',
            'contingent_worker_id',
            'compensation_effective_date',
            'emp_comp_summary_total_base_pay',
            'emp_comp_summary_total_salary_and_allowances',
            'emp_comp_summary_primary_compensation_basis',
            'annualized_total_base_pay',
            'annualized_total_salary_and_allowances',
            'annualized_primary_compensation_basis',
            'pay_group_frequency_total_base_pay',
            'pay_group_frequency_total_salary_and_allowances',
            'pay_group_frequency_primary_compensation_basis',
            'annualized_reporting_total_base_pay',
            'annualized_reporting_total_salary_and_allowances',
            'annualized_reporting_primary_compensation_basis',
            'hourly_frequency_total_base_pay',
            'compensation_reason_wid',
            'general_event_subcategory_id',
            'event_classification_subcategory_id',
            'change_job_subcategory_id',
            'paygroup_frequency_id',
            'paygroup_currency_id',
            'annualized_currency_id',
            'annualized_frequency_id',
            'annualized_reporting_frequency_id',
            'annualized_reporting_currency_id',
            'hourly_frequency_id',
            'hourly_frequency_currency_id',
            'summary_frequency_id',
            'summary_currency_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_workers_compensation_data')}} as get_workers_compensation_data
)
select
    worker_wid,
    worker_id,
    user_id,
    universal_id,
    employee_id,
    contingent_worker_id,
    compensation_effective_date,
    emp_comp_summary_total_base_pay,
    emp_comp_summary_total_salary_and_allowances,
    emp_comp_summary_primary_compensation_basis,
    annualized_total_base_pay,
    annualized_total_salary_and_allowances,
    annualized_primary_compensation_basis,
    pay_group_frequency_total_base_pay,
    pay_group_frequency_total_salary_and_allowances,
    pay_group_frequency_primary_compensation_basis,
    annualized_reporting_total_base_pay,
    annualized_reporting_total_salary_and_allowances,
    annualized_reporting_primary_compensation_basis,
    hourly_frequency_total_base_pay,
    compensation_reason_wid,
    general_event_subcategory_id,
    event_classification_subcategory_id,
    change_job_subcategory_id,
    paygroup_frequency_id,
    paygroup_currency_id,
    annualized_currency_id,
    annualized_frequency_id,
    annualized_reporting_frequency_id,
    annualized_reporting_currency_id,
    hourly_frequency_id,
    hourly_frequency_currency_id,
    summary_frequency_id,
    summary_currency_id,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    worker_comp
where
    1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where worker_wid = worker_comp.worker_wid)
    {%- endif %}
