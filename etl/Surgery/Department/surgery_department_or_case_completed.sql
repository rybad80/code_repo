with timestamps as (
    select
        surgery_department_or_case_all.or_key,
        or_case.case_begin_dt + cast(or_case.setup_offset || ' minutes' as interval) as scheduled_start_date,
        min(case when events.src_id = 5   then or_log_case_times.event_in_dt end) as wheels_in_time,
        min(case when events.src_id = 7   then or_log_case_times.event_in_dt end) as procedure_start_date,
        min(case when events.src_id = 8   then or_log_case_times.event_in_dt end) as procedure_close_date,
        min(case when events.src_id = 10  then or_log_case_times.event_in_dt end) as wheels_out_time
    from
        {{ref('surgery_department_or_case_all')}} as surgery_department_or_case_all
        left join {{source('cdw', 'or_case')}} as or_case
            on or_case.or_case_key = surgery_department_or_case_all.case_key
        left join {{source('cdw', 'or_log_case_times')}} as or_log_case_times
            on or_log_case_times.log_key = surgery_department_or_case_all.log_key
        left join {{source('cdw', 'cdw_dictionary')}} as events
            on events.dict_key = or_log_case_times.dict_or_pat_event_key
    group by
        surgery_department_or_case_all.or_key,
        or_case.case_begin_dt,
        or_case.setup_offset
),
assisting_staff as (
    select
        surgery_department_or_case_all.or_key,
        '[' || group_concat(
            '{"provider_type": "' || lower(provider.prov_type)
            || '", "provider_id": "' || provider.prov_id
            || '", "provider_name": "' || initcap(provider.full_nm) || '"}',
            ','
        ) || ']' as assisting_staff,
        max(case when lower(provider.prov_type) = 'resident' then 1 else 0 end) as assisting_resident_ind,
        max(case when lower(provider.prov_type) = 'fellow' then 1 else 0 end) as assisting_fellow_ind
    from
        {{ref('surgery_department_or_case_all')}} as surgery_department_or_case_all
        inner join {{source('cdw', 'or_log_staff')}} as or_log_staff
            on or_log_staff.log_key = surgery_department_or_case_all.or_key
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = or_log_staff.surg_prov_key
    where
        lower(provider.prov_type) in (
            'fellow',
            'medical student',
            'physician assistant',
            'registered nurse',
            'resident'
        )
    group by
        surgery_department_or_case_all.or_key
),
ssi_harm as (
    select
        fact_ip_ssi.log_key
    from
        {{source('cdw', 'fact_ip_ssi')}} as fact_ip_ssi
    group by
        fact_ip_ssi.log_key
),
followup as (
    select
        surgery_department_or_case_procedure.or_key,
        min(encounter_office_visit_all.encounter_date) as first_followup_appointment_date
    from
        {{ref('surgery_department_or_case_procedure')}} as surgery_department_or_case_procedure
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = surgery_department_or_case_procedure.visit_key
        inner join {{ref('encounter_office_visit_all')}} as encounter_office_visit_all
            on encounter_office_visit_all.pat_key = surgery_department_or_case_procedure.pat_key
        inner join {{ref('lookup_surgery_division_department')}} as lookup_surgery_division_department
            on lookup_surgery_division_department.department_id = encounter_office_visit_all.department_id
    where
        encounter_office_visit_all.encounter_date > date(stg_encounter.hospital_discharge_date)
        and (
            encounter_office_visit_all.cancel_noshow_ind = 0
            or encounter_office_visit_all.encounter_date >= current_date
        ) and lookup_surgery_division_department.surgery_division
            = surgery_department_or_case_procedure.surgery_division
    group by
        surgery_department_or_case_procedure.or_key
)
select
    surgery_department_or_case_all.or_key,
    surgery_department_or_case_all.mrn,
    surgery_department_or_case_all.patient_name,
    surgery_department_or_case_all.csn,
    surgery_department_or_case_all.dob,
    surgery_department_or_case_all.sex,
    surgery_department_or_case_all.race_ethnicity,
    surgery_department_or_case_all.surgery_age_years,
    surgery_department_or_case_all.surgery_date,
    surgery_department_or_case_all.log_id,
    surgery_department_or_case_all.location,
    surgery_department_or_case_all.location_group,
    surgery_department_or_case_all.room,
    surgery_department_or_case_all.patient_class,
    surgery_department_or_case_all.n_panels,
    surgery_department_or_case_all.n_procedures,
    surgery_department_or_case_all.primary_surgeon,
    surgery_department_or_case_all.primary_surgeon_provider_id,
    surgery_department_or_case_all.all_surgeons,
    surgery_department_or_case_all.primary_service,
    surgery_department_or_case_all.primary_surgery_division,
    surgery_department_or_case_all.all_services,
    surgery_department_or_case_all.all_procedures,
    assisting_staff.assisting_staff,
    assisting_staff.assisting_resident_ind,
    assisting_staff.assisting_fellow_ind,
    surgery_department_or_case_all.request_date,
    surgery_department_or_case_all.first_booked_date,
    surgery_department_or_case_all.n_days_request_to_booked,
    surgery_department_or_case_all.n_days_booked_to_surgery,
    surgery_department_or_case_all.scheduled_duration_mins,
    extract(epoch from timestamps.wheels_out_time - timestamps.wheels_in_time) / 60 as actual_duration_mins,
    (actual_duration_mins / cast(scheduled_duration_mins as float)) - 1 as duration_deviation_pct,
    case
        when duration_deviation_pct is null then null
        when abs(duration_deviation_pct) >= 0.2 then 1
        else 0
    end as duration_deviation_20_pct_ind,
    timestamps.scheduled_start_date,
    timestamps.wheels_in_time as actual_start_date,
    round(
        hour(timestamps.scheduled_start_date) + minute(timestamps.scheduled_start_date) / 60.0,
        2
    ) as scheduled_start_time_of_day,
    round(
        hour(timestamps.wheels_in_time) + minute(timestamps.wheels_in_time) / 60.0,
        2
    ) as actual_start_time_of_day,
    extract(epoch from timestamps.wheels_in_time - timestamps.scheduled_start_date) / 60 as late_start_mins,
    case when late_start_mins >= 15 then 1 else 0 end as late_start_15_min_ind,
    round(
        extract(
            epoch from stg_encounter.hospital_discharge_date - timestamps.wheels_out_time
        ) / 60.0 / 60.0,
        1
    ) as postop_los_hours,
    round(
        extract(
            epoch from stg_encounter.hospital_discharge_date - timestamps.wheels_out_time
        ) / 60.0 / 60.0 / 24.0,
        1
    ) as postop_los_days,
    case when stg_encounter_inpatient.visit_key is not null then 1 else 0 end as inpatient_ind,
    case when stg_encounter_ed.visit_key is not null then 1 else 0 end as ed_ind,
    coalesce(stg_encounter_inpatient.icu_ind, 0) as icu_ind,
    surgery_department_or_case_all.multiple_surgery_visit_ind,
    case when ssi_harm.log_key is not null then 1 else 0 end as ssi_harm_ind,
    case
        when followup.first_followup_appointment_date - date(stg_encounter.hospital_discharge_date) < 45
        then 1
        else 0
    end as followup_in_6_wk_ind,
    case
        when followup.first_followup_appointment_date - date(stg_encounter.hospital_discharge_date) < 90
        then 1
        else 0
    end as followup_in_90_day_ind,
    surgery_department_or_case_all.patient_address_seq_num,
    surgery_department_or_case_all.patient_address_zip_code,
    diagnosis_medically_complex.complex_chronic_condition_ind,
    diagnosis_medically_complex.medically_complex_ind,
    case
        when lower(stg_encounter_payor.payor_group) in ('medical assistance', 'government')
        then 1
        else 0
    end as govt_payor_ind,
    surgery_department_or_case_all.fiscal_year,
    surgery_department_or_case_all.calendar_year,
    surgery_department_or_case_all.calendar_month,
    surgery_department_or_case_all.visit_key,
    surgery_department_or_case_all.pat_key,
    surgery_department_or_case_all.case_key,
    surgery_department_or_case_all.primary_surgeon_prov_key,
    surgery_department_or_case_all.proc_ord_key
from
    {{ref('surgery_department_or_case_all')}} as surgery_department_or_case_all
    inner join {{source('cdw', 'or_case')}} as or_case
        on or_case.or_case_key = surgery_department_or_case_all.case_key
    left join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = surgery_department_or_case_all.visit_key
    left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter_payor.visit_key = surgery_department_or_case_all.visit_key
    left join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
        on stg_encounter_inpatient.visit_key = surgery_department_or_case_all.visit_key
    left join {{ref('stg_encounter_ed')}} as stg_encounter_ed
        on stg_encounter_ed.visit_key = surgery_department_or_case_all.visit_key
        and stg_encounter_ed.ed_patients_seen_ind = 1
    left join timestamps
        on timestamps.or_key = surgery_department_or_case_all.or_key
    left join assisting_staff
        on assisting_staff.or_key = surgery_department_or_case_all.or_key
    left join ssi_harm
        on ssi_harm.log_key = surgery_department_or_case_all.or_key
    left join {{ref('diagnosis_medically_complex')}} as diagnosis_medically_complex
        on diagnosis_medically_complex.visit_key = surgery_department_or_case_all.visit_key
    left join followup
        on followup.or_key = surgery_department_or_case_all.or_key
where
    surgery_department_or_case_all.case_status = 'Completed'
