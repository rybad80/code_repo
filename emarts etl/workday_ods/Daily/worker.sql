{{
    config(
        materialized = 'incremental',
        unique_key = 'worker_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['worker_wid', 'worker_id', 'user_id', 'universal_id', 'employee_id', 'contingent_worker_id', 'gender_code', 'ethnicity_id', 'legal_formated_name', 'legal_reporting_name', 'legal_first_name', 'legal_middle_name', 'legal_last_name', 'legal_secondary_last_name', 'legal_tertiary_last_name', 'preferred_formated_name', 'preferred_reporting_name', 'preferred_first_name', 'preferred_middle_name', 'preferred_last_name', 'preferred_secondary_last_name', 'preferred_tertiary_last_name', 'birth_date', 'death_date', 'city_of_birth', 'marital_status_date', 'hispanic_or_latino_ind', 'tobacco_use_ind', 'md5', 'upd_dt', 'upd_by']
    )
}}
with worker_data as (
    select distinct
        get_workers_personal_data.worker_worker_reference_wid as worker_wid,
        get_workers_personal_data.worker_worker_data_worker_id as worker_id,
        get_workers_personal_data.worker_worker_data_user_id as user_id,
        get_workers_personal_data.worker_worker_data_universal_id as universal_id,
        cast(cast(get_workers_personal_data.worker_worker_reference_employee_id as int) as varchar(50)) as employee_id,
        cast(cast(get_workers_personal_data.worker_worker_reference_contingent_worker_id as int) as varchar(50)) as contingent_worker_id,
        get_workers_personal_data.personal_information_data_personal_information_for_country_data_country_personal_information_data_gender_reference_gender_code as gender_code,
        get_workers_personal_data.personal_information_for_country_data_country_personal_information_data_ethnicity_reference_ethnicity_id as ethnicity_id,
        get_workers_personal_data.personal_data_name_data_legal_name_data_name_detail_data_formatted_name as legal_formated_name,
        get_workers_personal_data.personal_data_name_data_legal_name_data_name_detail_data_reporting_name as legal_reporting_name,
        get_workers_personal_data.personal_data_name_data_legal_name_data_name_detail_data_first_name as legal_first_name,
        get_workers_personal_data.personal_data_name_data_legal_name_data_name_detail_data_middle_name as legal_middle_name,
        get_workers_personal_data.personal_data_name_data_legal_name_data_name_detail_data_last_name as legal_last_name,
        get_workers_personal_data.personal_data_name_data_legal_name_data_name_detail_data_secondary_last_name as legal_secondary_last_name,
        null as legal_tertiary_last_name,
        get_workers_personal_data.personal_data_name_data_preferred_name_data_name_detail_data_formatted_name as preferred_formated_name,
        get_workers_personal_data.personal_data_name_data_preferred_name_data_name_detail_data_reporting_name as preferred_reporting_name,
        get_workers_personal_data.personal_data_name_data_preferred_name_data_name_detail_data_first_name as preferred_first_name,
        get_workers_personal_data.personal_data_name_data_preferred_name_data_name_detail_data_middle_name as preferred_middle_name,
        get_workers_personal_data.personal_data_name_data_preferred_name_data_name_detail_data_last_name as preferred_last_name,
        get_workers_personal_data.personal_data_name_data_preferred_name_data_name_detail_data_secondary_last_name as preferred_secondary_last_name,
        null as preferred_tertiary_last_name,
        to_timestamp(get_workers_personal_data.worker_worker_data_personal_data_personal_information_data_birth_date, 'YYYY-MM-DD') as birth_date,
        to_timestamp(get_workers_personal_data.worker_worker_data_personal_data_personal_information_data_date_of_death, 'YYYY-MM-DD') as death_date,
        null as city_of_birth,
        cast(null as timestamp) as marital_status_date,
        coalesce(cast(get_workers_personal_data.personal_information_data_personal_information_for_country_data_country_personal_information_data_hispanic_or_latino as int), 0) as hispanic_or_latino_ind,
        coalesce(cast(get_workers_personal_data.worker_worker_data_personal_data_tobacco_use as int), 2) as tobacco_use_ind,
        cast({{
            dbt_utils.surrogate_key([
                'worker_wid',
                'worker_id',
                'user_id',
                'universal_id',
                'employee_id',
                'contingent_worker_id',
                'gender_code',
                'ethnicity_id',
                'legal_formated_name',
                'legal_reporting_name',
                'legal_first_name',
                'legal_middle_name',
                'legal_last_name',
                'legal_secondary_last_name',
                'legal_tertiary_last_name',
                'preferred_formated_name',
                'preferred_reporting_name',
                'preferred_first_name',
                'preferred_middle_name',
                'preferred_last_name',
                'preferred_secondary_last_name',
                'preferred_tertiary_last_name',
                'birth_date',
                'death_date',
                'city_of_birth',
                'marital_status_date',
                'hispanic_or_latino_ind',
                'tobacco_use_ind'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_workers_personal_data')}} as get_workers_personal_data
)
select
    worker_wid,
    worker_id,
    user_id,
    universal_id,
    employee_id,
    contingent_worker_id,
    gender_code,
    ethnicity_id,
    legal_formated_name,
    legal_reporting_name,
    legal_first_name,
    legal_middle_name,
    legal_last_name,
    legal_secondary_last_name,
    legal_tertiary_last_name,
    preferred_formated_name,
    preferred_reporting_name,
    preferred_first_name,
    preferred_middle_name,
    preferred_last_name,
    preferred_secondary_last_name,
    preferred_tertiary_last_name,
    birth_date,
    death_date,
    city_of_birth,
    marital_status_date,
    hispanic_or_latino_ind,
    tobacco_use_ind,
    md5,
    create_dt,
    create_by,
    upd_dt,
    upd_by
from
    worker_data
where
    1 = 1     
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                worker_wid = worker_data.worker_wid
            )
    {%- endif %}
