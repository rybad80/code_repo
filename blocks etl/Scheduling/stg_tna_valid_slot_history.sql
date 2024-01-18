{%- call statement('current_date', fetch_result=True) -%}
    select
        year(current_date)*10000 + month(Current_date)*100 + day(current_date)
{%- endcall -%}
{%- set current_date_key = load_result('current_date')['data'][0][0] -%}

with office_visit_grouper as (
    select
        department_id,
        max(physician_app_psych_visit_ind) as physician_app_psych_visit_ind
    from
        {{ref('stg_office_visit_grouper')}}
    group by
        department_id
),

master_date as (
    select
        master_date.dt_key,
        master_date.full_dt as as_of_date,
        master_date.c_yyyy as slot_calendar_year,
        master_date.f_yyyy as slot_fiscal_year,
        master_date.c_wk_start_dt as slot_week_start,
        master_date.c_wk_end_dt as slot_week_end
    from {{ source('cdw', 'master_date')}}  as master_date
    where
        master_date.dt_key >= 20220120
        and master_date.dt_key <= {{current_date_key}}
),

most_recent_status as (
    select
        master_date.dt_key,
        master_date.as_of_date,
        master_date.slot_calendar_year,
        master_date.slot_fiscal_year,
        master_date.slot_week_start,
        master_date.slot_week_end,
        case
            when lower(scheduling_provider_slot_history.specialty_name) = 'general pediatrics'
                and scheduling_provider_slot_history.intended_use_id = '1013'
                then 'PRIMARY CARE'
            when lower(scheduling_provider_slot_history.specialty_name) = 'behavioral health services'
                and lower(scheduling_provider_slot_history.department_name) like '%neonatal fol up'
                then 'NEONATAL FOLLOW UP'
            else scheduling_provider_slot_history.specialty_name end as specialty_name,
        case -- *start of specialty care
            when ((--provider type definition 
                lower(scheduling_provider_slot_history.prov_type) = 'physician' --physician
                or lower(scheduling_provider_slot_history.prov_type) in
                    ('midwife', 'nurse practitioner',
                    'nurse anesthetist', 'clinical nurse specialist',
                    'physician assistant') -- APP
                or lower(scheduling_provider_slot_history.prov_type) like '%psycholog%' --pyschologist
            )
            and (office_visit_grouper.physician_app_psych_visit_ind = 1
            and lower(scheduling_provider_slot_history.specialty_name) not in
            ('cardiovascular surgery', 'obstetrics', 'multidisciplinary',
            'gi/nutrition', 'family planning')
                and scheduling_provider_slot_history.intended_use_id in ('1009')))
            /*
            * Licensed social workers for Behavioral Health are included in this grouper
            */
            or (lower(scheduling_provider_slot_history.specialty_name) = 'behavioral health services'
                and lower(scheduling_provider_slot_history.prov_type) = 'social worker'
                and scheduling_provider_slot_history.prov_title = 'LCSW'
                and office_visit_grouper.physician_app_psych_visit_ind = 1) -- end of specialty care
            or (lower(scheduling_provider_slot_history.specialty_name) in -- start of Ancillary Services
            ('physical therapy', 'speech', 'audiology',
            'occupational therapy', 'clinical nutrition')
            and (scheduling_provider_slot_history.intended_use_id in ('1009'))) -- end of Ancillary Services
            or  ((--provider type definition -- start of primary care
                lower(scheduling_provider_slot_history.prov_type) = 'physician' --physician
                or lower(scheduling_provider_slot_history.prov_type) = 'nurse practitioner' -- APP
            ) and scheduling_provider_slot_history.intended_use_id in ('1013')
            and lower(scheduling_provider_slot_history.department_name) not like '%north hills%')
            then 1 else 0 -- end of primary care*
        end as physician_app_psych_visit_ind,
        scheduling_provider_slot_history.department_id,
        scheduling_provider_slot_history.department_name,
        scheduling_provider_slot_history.prov_id,
        scheduling_provider_slot_history.provider_name,
        scheduling_provider_slot_history.appt_block_c,
        scheduling_provider_slot_history.appt_block_nm,
        scheduling_provider_slot_history.revenue_location_group,
        scheduling_provider_slot_history.slot_begin_time,
        scheduling_provider_slot_history.available_ind,
        scheduling_provider_slot_history.open_ind,
        scheduling_provider_slot_history.fill_rate_incl_ind,
        scheduling_provider_slot_history.tna_fill_rate_incl_ind,
        scheduling_provider_slot_history.slot_lgth_min,
        scheduling_provider_slot_history.slot_date,
        scheduling_provider_slot_history.valid_from_date_key,
        scheduling_provider_slot_history.valid_to_date_key,
        scheduling_provider_slot_history.slot_begin_date_key,
        days_between(scheduling_provider_slot_history.slot_date, master_date.as_of_date) as days_to_slot,
        (extract(days from scheduling_provider_slot_history.slot_begin_time - master_date.as_of_date))
            - ((extract(days from scheduling_provider_slot_history.slot_begin_time - master_date.as_of_date) / 7) * 2)
            - (case when extract(dow from as_of_date) = 1 then 1 else 0 end)
            - (case when extract(dow from slot_begin_time) = 7 then 1 else 0 end) as weekdays_to_slot,
        case
            when date(slot_begin_time) = date(as_of_date)
            then 1 else 0
        end as same_day_slot_ind,
        case
            when dt_key = valid_to_date_key
                then 0 else 1
        end as most_recent_status_ind
    from {{ref('scheduling_provider_slot_history')}} as scheduling_provider_slot_history
    inner join master_date as master_date
        on master_date.dt_key >= scheduling_provider_slot_history.valid_from_date_key
        and master_date.dt_key <= scheduling_provider_slot_history.valid_to_date_key
    left join office_visit_grouper as office_visit_grouper
        on office_visit_grouper.department_id = scheduling_provider_slot_history.department_id
    where
        exists (
            select
                valid_block_lookup.dept_block_lookup_key
            from
                {{ref('stg_tna_valid_block_lookup')}} as valid_block_lookup
            where
                scheduling_provider_slot_history.dept_block_lookup_key = valid_block_lookup.dept_block_lookup_key
                and valid_block_lookup.valid_slot_ind = 1
        )
        and scheduling_provider_slot_history.slot_begin_date_key >= master_date.dt_key
)

select
    dt_key,
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
    available_ind,
    open_ind,
    fill_rate_incl_ind,
    tna_fill_rate_incl_ind,
    slot_lgth_min,
    slot_date,
    valid_from_date_key,
    valid_to_date_key,
    slot_begin_date_key,
    days_to_slot,
    weekdays_to_slot,
    same_day_slot_ind,
    most_recent_status_ind,
    {{
    dbt_utils.surrogate_key([
        'slot_begin_time',
        'department_id',
        'prov_id',
        'as_of_date'
        ])
    }} as slot_prov_dept_key
from
    most_recent_status
where
    available_ind = 1 --checks that slot is not unavailable or on hold
    and open_ind = 1 --checks that slot is not filled with scheduled encounter
    and most_recent_status_ind = 1
    and tna_fill_rate_incl_ind = 1
