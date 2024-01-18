{{ config(
    materialized = 'incremental',
    unique_key = 'period_schedule_wid',
    incremental_strategy = 'merge',
    merge_update_columns = [
	    'period_schedule_wid',
        'period_schedule_id',
        'period_schedule_name',
        'payment_date_auto_adjust_saturday',
        'payment_date_auto_adjust_sunday',
        'allow_timesheet_changes_ind',
        'frequency_id',
        'md5', 'upd_dt', 'upd_by'
    ]
) }}
select distinct
    period_schedule_reference_wid as period_schedule_wid,
    period_schedule_reference_period_schedule_id as period_schedule_id,
    period_schedule_data_period_schedule_name as period_schedule_name,
    period_schedule_data_payment_date_auto_adjust_saturday as payment_date_auto_adjust_saturday,
    period_schedule_data_payment_date_auto_adjust_sunday as payment_date_auto_adjust_sunday,
    period_schedule_data_allow_timesheet_changes as allow_timesheet_changes_ind,
    frequency_reference_frequency_id as frequency_id,
    cast({{
        dbt_utils.surrogate_key([
            'period_schedule_wid',
            'period_schedule_id',
            'period_schedule_name',
            'payment_date_auto_adjust_saturday',
            'payment_date_auto_adjust_sunday',
            'allow_timesheet_changes_ind',
            'frequency_id'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{ source('workday_ods', 'get_period_schedules') }} as get_period_schedules
where
    1=1
    {%- if is_incremental() %}
        and md5 not in (
            select
                md5
            from
                {{ this }}
            where
                period_schedule_wid = get_period_schedules.period_schedule_reference_wid
        )
    {%- endif %}
