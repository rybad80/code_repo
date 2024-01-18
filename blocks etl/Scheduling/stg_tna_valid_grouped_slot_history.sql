{{
    config(
        materialized='incremental',
        unique_key = 'slot_prov_dept_key'
    )
}}

select
    slot_prov_dept_key,
    as_of_date,
    slot_calendar_year,
    slot_fiscal_year,
    slot_week_start,
    slot_week_end,
    specialty_name,
    physician_app_psych_visit_ind,
    department_id,
    department_name,
    prov_id,
    provider_name,
    group_concat(appt_block_c, ',') as appt_block_c,
    group_concat(appt_block_nm, ',') as appt_block_nm,
    revenue_location_group,
    slot_begin_time,
    slot_lgth_min,
    slot_date,
    days_to_slot,
    weekdays_to_slot,
    same_day_slot_ind
from
    {{ref('stg_tna_valid_slot_history')}}
where
    1 = 1
{% if is_incremental() %}
    and as_of_date > (select (max(as_of_date) - 3) from {{ this }})
{% endif %}
group by
    slot_prov_dept_key,
    as_of_date,
    slot_calendar_year,
    slot_fiscal_year,
    slot_week_start,
    slot_week_end,
    specialty_name,
    physician_app_psych_visit_ind,
    department_id,
    department_name,
    prov_id,
    provider_name,
    revenue_location_group,
    slot_begin_time,
    slot_lgth_min,
    slot_date,
    days_to_slot,
    weekdays_to_slot,
    same_day_slot_ind
