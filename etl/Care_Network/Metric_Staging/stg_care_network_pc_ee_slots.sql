select
    {{
        dbt_utils.surrogate_key([
            'dept_key',
            'prov_key',
            'slot_start_time'
        ])
    }} as primary_key,
    'Fill Rate' as metric_name,
    'pc_ee_slot_fill_rate' as metric_id,
    slot_start_time,
    prov_key,
    dept_key,
    encounter_date,
    slot_length_minute,
    appt_lenth_minute,
    revenue_location_group,
    specialty_name,
    department_name,
    department_id,
    provider_name,
    provider_type,
    provider_id,
    csn,
    pat_key,
    visit_key,
    visit_type,
    visit_type_id,
    slot_appointment_status,
    slot_appointment_status_id,
    slot_appointment_block,
    slot_status,
    unavailable_reason,
    hold_reason,
    slot_status_detail,
    unavailable_hold_ind,
    day_unavailable_ind,
    time_unavailable_ind,
    available_ind,
    open_ind,
    scheduled_ind,
    specialty_care_slot_ind,
    primary_care_slot_ind,
    evening_appointment_ind,
    weekend_appointment_ind,
    fill_rate_incl_ind

from 
    {{ref('scheduling_provider_slot_status')}}

where
    -- A Primary Care Location
    primary_care_slot_ind = 1
    and fill_rate_incl_ind = 1
