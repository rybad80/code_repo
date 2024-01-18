{{ config(meta = {
    'critical': true
}) }}

/*
This code takes the underlying filters for encounter_speciality_care block
and applies them to visits of all appointment statuses (incl. no show and canceled visits)
*/
with encounter_specialty_care_all as ( --noqa: L018
    --region stg_encounter_outpatient block filtered to specialty care criteria
    select
        stg_encounter_outpatient.*,
        stg_encounter_outpatient.telehealth_ind as video_visit_ind
    from
        {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
    where
        lower(stg_encounter_outpatient.intended_use_name) = 'outpatient specialty care'
        and stg_encounter_outpatient.encounter_date is not null
        -- patient classes: outpatient/recurring outpatient/not applicable
        and stg_encounter_outpatient.patient_class_id in ('2', '6', '0')
        -- encounter types: office visit/appointment/hospital encounter/outpatient/sunday office visit
        and stg_encounter_outpatient.encounter_type_id in ('101', '50', '3', '152', '204')
), --noqa: L018

next_encounter_all as (
    --region identify next completed/arrived/scheduled visits based on specialty and visit type
    select
        encounter_specialty_care_all.visit_key,
        next_appointment.encounter_date as next_specialty_encounter_date,
        next_appointment.department_name as next_specialty_department_name,
        next_appointment.provider_name as next_specialty_provider_name,
        next_appointment.visit_type as next_specialty_visit_type,
        next_appointment.visit_type_id as next_specialty_visit_type_id,
        next_appointment.visit_key as next_specialty_visit_key,
        row_number() over(
            partition by encounter_specialty_care_all.visit_key
            order by next_appointment.encounter_date, next_appointment.visit_key
        ) as visit_num
    from
        encounter_specialty_care_all as next_appointment
        inner join {{ref('stg_department_all')}} as next_department_all
            on next_appointment.dept_key = next_department_all.dept_key
        inner join encounter_specialty_care_all on
            next_department_all.specialty_name = encounter_specialty_care_all.specialty_name
            and next_appointment.pat_key = encounter_specialty_care_all.pat_key
    where
        --next visit is on or after the min_date (earliest of encounter date or appointment cancel date)
        next_appointment.encounter_date >= encounter_specialty_care_all.min_date
        and (next_appointment.visit_type_id = encounter_specialty_care_all.visit_type_id --visit types align
            or (lower(encounter_specialty_care_all.visit_type) like '%new%'
                and lower(next_appointment.visit_type) like '%fol%') --new patient becomes follow-up patient
            or (lower(encounter_specialty_care_all.visit_type) like '%npv%'
            --new patient becomes follow-up patient
                and lower(next_appointment.visit_type) like '%fol%')
                --alternating between in-person and telemedicine
            or encounter_specialty_care_all.video_telephone_visit_ind = 1
            --alternating between in-person and telemedicine
            or next_appointment.video_telephone_visit_ind = 1
            --any visit type may follow a second opinion
            or lower(encounter_specialty_care_all.visit_type) like '%sec%opin%'
            --an internal second opinion may follow any visit type
            or lower(next_appointment.visit_type) like '%internal%sec%opin%'
            --any visit type may follow an injection in clinic (Rehab)
            or lower(encounter_specialty_care_all.visit_type) like 'injection in%'
            --an injection in clinic may follow any visit type (Rehab)
            or lower(next_appointment.visit_type) like 'injection in%'
            )
    and next_appointment.appointment_status_id in (1, 2, 6) --scheduled, completed, arrived
    and encounter_specialty_care_all.visit_key != next_appointment.visit_key
--end region
),

follow_up_temp as ( --region first step for identifying follow-up request
    select distinct
        visit_follow_up_disp.pat_key,
        visit_follow_up_disp.visit_key,
        follow_up_disp,
        visit_follow_up_disp.disp_return_note,
        case when dim_unit_type.unit_type_id = 1 then date(disp_entered_dt + disp_num_units)
                when dim_unit_type.unit_type_id = 2 then date(disp_entered_dt + (disp_num_units * 7))
                when dim_unit_type.unit_type_id = 3 then date(add_months(disp_entered_dt, disp_num_units))
                when dim_unit_type.unit_type_id = 4 then date(add_months(disp_entered_dt, (disp_num_units) * 12))
                when dim_unit_type.unit_type_id = 6 then date(disp_entered_dt + disp_num_units)
            end as follow_up_date,
        case when unit_type_id = 0 then 1 else 0 end as follow_up_with_no_date
    from
        encounter_specialty_care_all
        inner join {{source('cdw', 'visit_follow_up_disp')}} as visit_follow_up_disp
            on encounter_specialty_care_all.visit_key = visit_follow_up_disp.visit_key
        left join  {{source('cdw', 'dim_unit_type')}} as dim_unit_type
            on visit_follow_up_disp.dim_disp_unit_type_key = dim_unit_type.dim_unit_type_key
    --end region
),

follow_up as ( --region identify unique follow-up request from provider for next visit
    select
          follow_up_temp.visit_key,
          max(follow_up_temp.follow_up_disp) as follow_up_disposition,
          max(follow_up_temp.disp_return_note) as disposition_return_note,
          min(follow_up_temp.follow_up_date) as follow_up_date,
          max(follow_up_temp.follow_up_with_no_date) as follow_up_no_date_ind
    from
        follow_up_temp
    where follow_up_temp.follow_up_date is not null
    or follow_up_temp.follow_up_with_no_date = 1
    group by follow_up_temp.visit_key
    -- end region
),

 secondary_provider as ( --region non-resource provider in visit_appointment
    select
        encounter_specialty_care_all.visit_key,
        provider.full_nm as secondary_provider_name,
        provider.prov_type as secondary_provider_type,
        provider.prov_key as secondary_prov_key
    from encounter_specialty_care_all
    inner join {{source('cdw', 'visit_appointment')}} as visit_appointment
        on encounter_specialty_care_all.visit_key = visit_appointment.visit_key
    inner join {{source('cdw', 'provider')}} as provider on visit_appointment.prov_key = provider.prov_key
    where visit_appointment.seq_num = 2
    and lower(provider.prov_type) != 'resource'
    -- end region
),

reason_for_visit as ( --region for reason comment related to visit
    select
        visit_reason.visit_key,
        visit_reason.rsn_cmt
    from
        {{source('cdw', 'visit_reason')}} as visit_reason
    where
        visit_reason.seq_num = 1
    -- end region
)

select
    encounter_specialty_care_all.visit_key,
    encounter_specialty_care_all.patient_name,
    encounter_specialty_care_all.mrn,
    encounter_specialty_care_all.dob,
    encounter_specialty_care_all.csn,
    encounter_specialty_care_all.encounter_date,
    encounter_specialty_care_all.appointment_date,
    encounter_specialty_care_all.appointment_made_date,
    encounter_specialty_care_all.original_appointment_made_date,
    encounter_specialty_care_all.appointment_entry_employee,
    encounter_specialty_care_all.appointment_entry_employee_id,
    encounter_specialty_care_all.appointment_cancel_date,
    encounter_specialty_care_all.scheduled_to_encounter_days as appointment_lag_days,
    encounter_specialty_care_all.npv_appointment_lag_days,
    encounter_specialty_care_all.specialty_name,
    encounter_specialty_care_all.department_name,
    encounter_specialty_care_all.department_id,
    encounter_specialty_care_all.revenue_location_group,
    encounter_specialty_care_all.chop_market,
    encounter_specialty_care_all.region_category,
    encounter_specialty_care_all.provider_name,
    encounter_specialty_care_all.provider_id,
    encounter_specialty_care_all.provider_type,
    encounter_specialty_care_all.physician_app_psych_visit_ind,
    secondary_provider.secondary_provider_name,
    secondary_provider.secondary_provider_type,
    secondary_provider.secondary_prov_key,
    encounter_specialty_care_all.visit_type,
    encounter_specialty_care_all.visit_type_id,
    encounter_specialty_care_all.appointment_status,
    encounter_specialty_care_all.appointment_status_id,
    encounter_specialty_care_all.last_completed_visit_key as last_specialty_completed_visit_key,
    encounter_specialty_care_all.last_completed_encounter_date as last_specialty_completed_encounter_date,
    next_encounter_all.next_specialty_visit_key,
    next_encounter_all.next_specialty_encounter_date,
    next_encounter_all.next_specialty_provider_name,
    next_encounter_all.next_specialty_department_name,
    next_encounter_all.next_specialty_visit_type,
    next_encounter_all.next_specialty_visit_type_id,
    stg_appointment_note_text.appointment_note_text,
    reason_for_visit.rsn_cmt as reason_for_visit,
    follow_up.follow_up_disposition,
    follow_up.disposition_return_note,
    follow_up.follow_up_date,
    follow_up.follow_up_no_date_ind,
    encounter_specialty_care_all.cancel_24hr_ind,
    encounter_specialty_care_all.cancel_48hr_ind,
    encounter_specialty_care_all.past_appointment_ind,
    encounter_specialty_care_all.noshow_ind,
    encounter_specialty_care_all.video_visit_ind,
    encounter_specialty_care_all.telephone_visit_ind,
    encounter_specialty_care_all.video_telephone_visit_ind,
    encounter_specialty_care_all.walkin_ind,
    encounter_specialty_care_all.online_scheduled_ind,
    encounter_specialty_care_all.mychop_scheduled_ind,
    encounter_specialty_care_all.scc_ind,
    encounter_specialty_care_all.new_patient_3yr_ind as new_to_specialty_3_yr_ind,
    encounter_specialty_care_all.npv_lag_incl_ind,
    encounter_specialty_care_all.international_ind,
    case
        when date_part('hour', encounter_specialty_care_all.appointment_date) < 8
        then 1
        else 0
        end as early_appointment_ind,
    case
        when date_part('hour', encounter_specialty_care_all.appointment_date) >= 16
            and encounter_specialty_care_all.department_id not in (62, 101022016, 101001076, 101003033, 101012070)
            /*excludes main sleep center, virtua sleep lab,kop sleep center (old)
            koph sleep center, main palliative care */
        then 1
        else 0
        end as evening_appointment_ind,
    case
        when date_part('dow', encounter_specialty_care_all.appointment_date) = 1 then 1
        when date_part('dow', encounter_specialty_care_all.appointment_date) = 7 then 1
        else 0
        end as weekend_appointment_ind,
    encounter_specialty_care_all.pat_key,
    encounter_specialty_care_all.dept_key,
    encounter_specialty_care_all.prov_key
from
    encounter_specialty_care_all
    left join next_encounter_all
        on encounter_specialty_care_all.visit_key = next_encounter_all.visit_key
        and next_encounter_all.visit_num = 1
    left join follow_up on encounter_specialty_care_all.visit_key = follow_up.visit_key
    left join secondary_provider on encounter_specialty_care_all.visit_key = secondary_provider.visit_key
    left join {{ref('stg_appointment_note_text')}} as stg_appointment_note_text
        on encounter_specialty_care_all.visit_key = stg_appointment_note_text.visit_key
    left join reason_for_visit on encounter_specialty_care_all.visit_key = reason_for_visit.visit_key
