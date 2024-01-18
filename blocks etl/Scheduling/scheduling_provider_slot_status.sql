{{ config(meta = {
    'critical': true
}) }}

select
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
    intended_use_name,
    provider_name,
    provider_type,
    provider_id,
    physician_app_psych_ind,
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
        --indicates a slot is NOT unavailable or on hold (denominator for Fill Rate metric)
    available_ind,
    open_ind,
    scheduled_ind,
    specialty_care_slot_ind,
    primary_care_slot_ind,
    ancillary_services_ind,
    evening_appointment_ind,
    weekend_appointment_ind,
    fill_rate_incl_ind

from
    {{ref('stg_scheduling_slot')}}

where
    provider_ind = 1
