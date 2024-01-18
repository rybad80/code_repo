{{
    config(
        materialized = 'incremental',
        unique_key = 'fiscal_period_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['fiscal_period_wid','fiscal_year_period','fiscal_period_start_date','fiscal_period_end_date','last_functionally_updated','last_functionally_updated_utc_offset','fiscal_year_wid','fiscal_year_descriptor','fiscal_schedule_wid','fiscal_schedule_id','fiscal_schedule_descriptor','curr_fy_current_fiscal_period_wid','curr_fy_prior_fiscal_period_wid', 'md5', 'upd_dt', 'upd_by']
    )
}}
with fiscal_sched as (
    select distinct
        fiscal_period_wid,
        fiscal_year_period,
        to_date(substring(fiscal_period_start_date,1,10),'yyyy-mm-dd') as fiscal_period_start_date,
        to_date(substring(fiscal_period_end_date,1,10),'yyyy-mm-dd') as fiscal_period_end_date,
        to_timestamp(substring(last_functionally_updated,1,10) || ' ' || substring(last_functionally_updated,12,12) , 'yyyy-mm-dd hh24:mi:ss.ms') as last_functionally_updated,
        substring(last_functionally_updated,25,5) as last_functionally_updated_utc_offset,
        fiscal_year_wid,
        fiscal_year as fiscal_year_descriptor,
        fiscal_schedule_wid,
        fiscal_schedule_ref_id as fiscal_schedule_id,
        fiscal_schedule as fiscal_schedule_descriptor,
        cf_current_fiscal_period_wid as curr_fy_current_fiscal_period_wid,
        cf_prior_fiscal_period_wid as curr_fy_prior_fiscal_period_wid
    from
        {{source('workday_ods', 'workday_fiscal_schedule')}} as workday_fiscal_schedule
)
select
    fiscal_period_wid,
    fiscal_year_period,
    fiscal_period_start_date,
    fiscal_period_end_date,
    last_functionally_updated,
    last_functionally_updated_utc_offset,
    fiscal_year_wid,
    fiscal_year_descriptor,
    fiscal_schedule_wid,
    fiscal_schedule_id,
    fiscal_schedule_descriptor,
    curr_fy_current_fiscal_period_wid,
    curr_fy_prior_fiscal_period_wid,
    cast({{
        dbt_utils.surrogate_key([
            'fiscal_period_wid',
            'fiscal_year_period',
            'fiscal_period_start_date',
            'fiscal_period_end_date',
            'last_functionally_updated',
            'last_functionally_updated_utc_offset',
            'fiscal_year_wid',
            'fiscal_year_descriptor',
            'fiscal_schedule_wid',
            'fiscal_schedule_id',
            'fiscal_schedule_descriptor',
            'curr_fy_current_fiscal_period_wid',
            'curr_fy_prior_fiscal_period_wid'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    fiscal_sched
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                fiscal_period_wid = fiscal_sched.fiscal_period_wid
        )
    {%- endif %}