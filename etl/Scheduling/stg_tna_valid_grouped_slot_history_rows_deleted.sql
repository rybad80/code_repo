with soft_deletes as (
    select
        slot_prov_dept_key
    from
        {{ref('stg_tna_valid_grouped_slot_history')}}
    where
        slot_prov_dept_key not in (
            select
                slot_prov_dept_key
            from
                {{ref('stg_tna_valid_slot_history')}}
            )
)

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
    appt_block_c,
    appt_block_nm,
    revenue_location_group,
    slot_begin_time,
    slot_lgth_min,
    slot_date,
    days_to_slot,
    weekdays_to_slot,
    same_day_slot_ind
from
    {{ref('stg_tna_valid_grouped_slot_history')}} as stg_tna_valid_grouped_slot_history
where
    not exists (
        select
            soft_deletes.slot_prov_dept_key
        from
            soft_deletes as soft_deletes
        where
            stg_tna_valid_grouped_slot_history.slot_prov_dept_key = soft_deletes.slot_prov_dept_key
    )
