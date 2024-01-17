{{ config(
	materialized='table',
	dist='visit_key',
	meta={
		'critical': true
	}
) }}

with dates as (
    select
        visit_ed_event.visit_key,
        min(
            case when master_event_type.event_id = 603 then visit_ed_event.event_dt else null end
        ) as start_visit_date,
        max(
            case when master_event_type.event_id = 40000 then visit_ed_event.event_dt else null end
        ) as assign_room_date,
        min(
            case when master_event_type.event_id = 601 then visit_ed_event.event_dt else null end
        ) as start_rooming_date,
        max(
            case when master_event_type.event_id = 602 then visit_ed_event.event_dt else null end
        ) as done_rooming_date,
        max(
            case when master_event_type.event_id = 604 then visit_ed_event.event_dt else null end
        ) as complete_visit_date
    from
        {{source('cdw', 'visit_ed_event')}} as visit_ed_event
        inner join {{source('cdw', 'master_event_type')}} as master_event_type
            on master_event_type.event_type_key = visit_ed_event.event_type_key
    where master_event_type.event_id in (
        600, -- Dept Check-in Comp
        601, -- Start Rooming
        602, -- Done Rooming
        603, -- Start Visit
        604, -- End Visit/Visit Complete
        40000, -- Assign to room
        57021 -- Dept Check-in St
    )
    group by visit_ed_event.visit_key
)

select
    stg_encounter.visit_key,
    stg_encounter.encounter_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.sex,
    stg_encounter.age_years,
    stg_encounter.age_months,
    stg_encounter.age_days,
    stg_encounter.encounter_date,
    stg_encounter.appointment_date,
    stg_encounter.appointment_made_date,
    case
        when stg_encounter.appointment_made_date <= stg_encounter.encounter_date
            then date(stg_encounter.encounter_date) - date(stg_encounter.appointment_made_date)
        else null
    end as scheduled_to_encounter_days, -- keeps only positive appointment lag
    stg_encounter.appointment_cancel_date,
    stg_department_all.specialty_name,
    stg_department_all.department_name,
    stg_department_all.department_center,
    cast(stg_department_all.department_id as bigint) as department_id,
    stg_encounter.patient_address_seq_num,
    stg_encounter.patient_address_zip_code,
    stg_encounter.appointment_status,
    stg_encounter.appointment_status_id,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.patient_class,
    stg_encounter.patient_class_id,
    stg_encounter.encounter_type_id,
    stg_department_all.intended_use_name,
    stg_department_all.intended_use_id,
    stg_department_all.revenue_location_group,
    stg_encounter_chop_market.chop_market,
    stg_encounter_chop_market.region_category,
    stg_encounter.cancel_24hr_ind,
    stg_encounter.cancel_48hr_ind,
    dates.start_visit_date,
    stg_encounter.check_in_date,
    dates.assign_room_date,
    dates.start_rooming_date,
    dates.done_rooming_date,
    stg_encounter.check_out_date,
    dates.complete_visit_date,
    stg_encounter.original_appointment_made_date,
    stg_encounter.hospital_discharge_date, -- for UC visits
    stg_encounter.scheduled_length_min,
    stg_encounter.encounter_close_date,
    stg_encounter.encounter_closed_ind,
    case
        when dates.start_visit_date is not null and dates.complete_visit_date is not null
            then round( extract( epoch
                from dates.complete_visit_date - dates.start_visit_date) / 60.0, 1) --noqa: PRS
        else null
    end as actual_length_min,
    stg_encounter.cancel_ind,
    stg_encounter.noshow_ind,
    stg_encounter.lws_ind, --left without seen
    stg_encounter.cancel_noshow_ind,
    stg_encounter.cancel_noshow_lws_ind,
    case -- appointment statuses: completed/arrived/no show
        when stg_encounter.appointment_status_id in (2, 6, 4) then 1
        when cancel_48hr_ind = 1 then 1 -- appointment status: cancelled, within 48hr
    else 0 end as past_appointment_ind,
    -- for calculating previous appointments
    coalesce(stg_encounter.appointment_date, stg_encounter.encounter_date) as appointment_date_no_blank,
    coalesce(min(
        date_trunc('day', stg_encounter.appointment_cancel_date),
        stg_encounter.encounter_date),
        stg_encounter.encounter_date + 1
    /* earliest of encounter date or appointment cancel date, for finding
    "next" completed appointment before original encounter date */
    ) as min_date,
    stg_department_all.scc_ind,
    case
        when stg_encounter_chop_market.chop_market = 'international'
    then 1 else 0 end as international_ind,
    case --1013 = Primary Care
        when stg_department_all.intended_use_id = '1013'
            and ( -- appointment statuses: completed/arrived/not applicable
                stg_encounter.appointment_status_id in (2, 6, -2)
                -- or scheduled visit in the future
                or (stg_encounter.appointment_status_id = 1
                    and stg_encounter.encounter_date >= current_date)
            )
    then 1 else 0 end as primary_care_ind,
    case --1009 = Specialty Care
        when stg_department_all.intended_use_id = '1009'
            and ( -- appointment statuses: completed/arrived/not applicable
                stg_encounter.appointment_status_id in (2, 6, -2)
                -- or scheduled visit in the future
                or (stg_encounter.appointment_status_id = 1
                    and stg_encounter.encounter_date >= current_date)
            )
    then 1 else 0 end as specialty_care_ind,
    case
        when lower(stg_department_all.specialty_name) = 'urgent care'
            and stg_encounter.encounter_type_id = 3 -- Hospital Encounter
            and pat_enc.calculated_enc_stat_c != 3 -- Invalid
        then 1 else 0 end as urgent_care_ind,
    -- Ensures this was a billed encounter, where patient saw provider
    case when stg_encounter.los_proc_cd like '99%' then 1 else 0 end as physician_service_level_ind,
    case when stg_encounter.encounter_type_id = '50' then 1 else 0 end as appointment_ind,
    case when stg_encounter.encounter_type_id = '101' then 1 else 0 end as office_visit_ind,
    case when stg_encounter.patient_class_id = '6' then 1 else 0 end as recurring_outpatient_ind,
    case when stg_encounter.los_proc_cd like '993%' then 1 else 0 end as well_visit_ind,
    case when stg_encounter.los_proc_cd like '992%' then 1 else 0 end as sick_visit_ind,
    case when stg_encounter.visit_type_id = '2152' then 1 else 0 end as telephone_visit_ind,
    stg_encounter.mychop_scheduled_ind, --indicator for appointments scheduled through mychop
    stg_encounter.walkin_ind,
    stg_encounter.online_scheduled_ind,
    case
        when age_months < 1
        and stg_encounter.cancel_noshow_ind != 1
        and stg_encounter.visit_key = first_value(stg_encounter.visit_key) over (
            partition by stg_encounter.pat_key, stg_encounter.cancel_noshow_ind
            order by stg_encounter.appointment_date, stg_encounter.encounter_date
        )
    then 1 else 0
    end as first_newborn_encounter_ind, -- at first visit of patients under 1 month old
    stg_encounter.pat_key,
    stg_encounter.patient_key,
    stg_encounter.dept_key,
    stg_department_all.department_key,
    stg_encounter.prov_key,
    stg_encounter.provider_key,
    stg_encounter.appt_entry_emp_key
from
    {{ref('stg_encounter')}} as stg_encounter
    inner join {{source('clarity_ods','pat_enc')}} as pat_enc
        on pat_enc.pat_enc_csn_id = stg_encounter.csn
    inner join {{ref('stg_department_all')}} as stg_department_all
        on stg_department_all.dept_key = stg_encounter.dept_key
    inner join {{ref('stg_encounter_chop_market')}} as stg_encounter_chop_market
        on stg_encounter_chop_market.visit_key = stg_encounter.visit_key
    left join dates
        on dates.visit_key = stg_encounter.visit_key
where
    -- patient classes: outpatient/recurring outpatient/not applicable
    stg_encounter.patient_class_id in ('2', '6', '0')
    -- encounter types: office visit/appointment/hospital/outpatient/sunday office visit
    and stg_encounter.encounter_type_id in ('101', '50', '3', '152', '204')
    and stg_encounter.encounter_date is not null
    -- PC and OP Specialty Care area
    and (stg_department_all.intended_use_id in ('1013', '1009')
        or lower(stg_department_all.specialty_name) = 'urgent care')
