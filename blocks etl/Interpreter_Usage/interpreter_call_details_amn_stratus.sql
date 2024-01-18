with date_conversion as (
    select
        amn_stratus.accountname as account_name,
        amn_stratus.sessionid as session_id,
        amn_stratus.invoicenumber as invoice_number,
        amn_stratus.language as requested_language,
        amn_stratus.calldate as call_date,
        amn_stratus.starttime as start_time,
        amn_stratus.duration as duration_min,
        timestamp(cast(amn_stratus.calldate as date), cast(amn_stratus.starttime as time)) as service_start_time,
        timestamp(cast(amn_stratus.calldate as date), cast(amn_stratus.starttime as time))
            + cast((cast(amn_stratus.duration as int) || 'minutes') as interval) as service_end_time,
        amn_stratus.sessiontype as session_type,
        amn_stratus.deviceid as device_id,
        amn_stratus.interpreterid as interpreter_id,
        amn_stratus.devicelocation1 as device_location_1,
        amn_stratus.devicelocation2 as device_location_2,
        amn_stratus.devicelocation3 as device_location_3,
        amn_stratus.opinumber as opi_number,
        amn_stratus.answerspeed as answer_speed,
        amn_stratus.callerid as caller_id,
        amn_stratus.noquestionsopi as noquestions_opi,
        amn_stratus.noquestionsvri as noquestions_vri,
        amn_stratus.patientmrnopi as patient_mrn_opi,
        amn_stratus.patientmrnvri as patient_mrn_vri,
        coalesce(replace(amn_stratus.patientmrnopi, '--', null), amn_stratus.patientmrnvri) as raw_mrn
    from
        {{source('ods', 'amn_stratus_interpreter_call_details')}} as amn_stratus
),

amn_data as (
    select
        date_conversion.account_name,
        date_conversion.session_id,
        date_conversion.invoice_number,
        date_conversion.requested_language,
        date_conversion.call_date,
        date_conversion.start_time,
        date_conversion.duration_min,
        date_conversion.service_start_time,
        date_conversion.service_end_time,
        date(date_conversion.service_start_time) as service_start_date,
        date(date_conversion.service_end_time) as service_end_date,
        date_conversion.session_type,
        date_conversion.device_id,
        date_conversion.interpreter_id,
        date_conversion.device_location_1,
        date_conversion.device_location_2,
        date_conversion.device_location_3,
        date_conversion.opi_number,
        date_conversion.answer_speed,
        date_conversion.caller_id,
        date_conversion.noquestions_opi,
        date_conversion.noquestions_vri,
        date_conversion.patient_mrn_opi,
        date_conversion.patient_mrn_vri,
        date_conversion.raw_mrn,
        stg_patient.pat_key,
        stg_patient.mrn
    from
        date_conversion as date_conversion
    left join {{ref('stg_patient')}} as stg_patient
        on stg_patient.mrn = date_conversion.raw_mrn
        and stg_patient.current_record_ind = 1 -- bringing current records from stg_patient
),

hospital_ed_encounters as ( --includes hostpial encounters with call between admission and discharge
    select
        'hospital_encounters' as source_1,
        amn_data.session_id,
        amn_data.start_time,
        amn_data.interpreter_id,
        stg_encounter.visit_key,
        stg_encounter.hospital_admit_date,
        stg_encounter.hospital_discharge_date
    from
        amn_data as amn_data
    inner join {{ref('stg_encounter')}} as stg_encounter
        on amn_data.pat_key = stg_encounter.pat_key
    where
        amn_data.service_start_time >= stg_encounter.hospital_admit_date
        and amn_data.service_start_time <= coalesce(stg_encounter.hospital_discharge_date, current_date)
        and (
            stg_encounter.visit_key in (select visit_key from {{ref('stg_encounter_inpatient')}})
            or stg_encounter.visit_key in (select visit_key from {{ref('stg_encounter_ed')}}
                where ed_patients_seen_ind = 1)
        )
),

single_day_encounters as (
    select
        'single_day_encounters' as source_2,
        amn_data.session_id,
        amn_data.start_time,
        amn_data.interpreter_id,
        stg_encounter.visit_key,
        stg_encounter.encounter_date,
        row_number() over (partition by amn_data.session_id
            order by stg_encounter.appointment_date, stg_encounter.visit_key) as line
    from
        amn_data as amn_data
    inner join {{ref('stg_encounter')}} as stg_encounter
        on amn_data.pat_key = stg_encounter.pat_key
    where
        amn_data.service_start_date = stg_encounter.encounter_date
        and (stg_encounter.encounter_type_id in (3, --hospital encounter
                                 70, --telephone encounter
                                101, -- office visit
                                 50) -- appointment
           or stg_encounter.appointment_status_id in('1', '2', '6') --scheduled, compelted, arrived;
           )
        and (stg_encounter.appointment_date is null
            or stg_encounter.appointment_date < amn_data.service_start_time)
            --if appointment time is populated, check that appointment started before call
        and stg_encounter.department_id != '1015002' --interpreter department
)

select
    amn_data.account_name,
    amn_data.session_id,
    amn_data.invoice_number,
    amn_data.requested_language,
    amn_data.call_date,
    amn_data.start_time,
    amn_data.service_start_time,
    amn_data.service_end_time,
    amn_data.session_type,
    amn_data.device_id,
    amn_data.duration_min,
    amn_data.interpreter_id,
    amn_data.device_location_1,
    amn_data.device_location_2,
    amn_data.device_location_3,
    amn_data.opi_number,
    amn_data.answer_speed,
    amn_data.caller_id,
    amn_data.noquestions_opi,
    amn_data.noquestions_vri,
    amn_data.patient_mrn_opi,
    amn_data.patient_mrn_vri,
    amn_data.raw_mrn,
    amn_data.pat_key,
    amn_data.mrn,
    coalesce(single_day_encounters.visit_key, hospital_ed_encounters.visit_key) as visit_key,
    hospital_ed_encounters.visit_key as hopsital_ed_visit_key,
    hospital_ed_encounters.hospital_admit_date,
    hospital_ed_encounters.hospital_discharge_date,
    single_day_encounters.visit_key as single_day_visit_key,
    single_day_encounters.encounter_date
from
    amn_data as amn_data
left join single_day_encounters as single_day_encounters
    on amn_data.session_id = single_day_encounters.session_id
    and amn_data.start_time = single_day_encounters.start_time
    and amn_data.interpreter_id = single_day_encounters.interpreter_id
    and line = 1
left join hospital_ed_encounters as hospital_ed_encounters
    on amn_data.session_id = hospital_ed_encounters.session_id
    and amn_data.start_time = hospital_ed_encounters.start_time
    and amn_data.interpreter_id = hospital_ed_encounters.interpreter_id
    and single_day_encounters.session_id is null
