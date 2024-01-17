with date_conversion as (
    select
        lsa_interpreter_call_details.recordnumber,
        lsa_interpreter_call_details.accountcode,
        lsa_interpreter_call_details.ani as caller_phone_number,
        lsa_interpreter_call_details.field1 as location,
        lsa_interpreter_call_details.field2 as caller_last_name,
        replace(replace(replace(lsa_interpreter_call_details.field3, '.', ''), '-', ''), ' ', '') as raw_mrn,
        lsa_interpreter_call_details.language as requested_language,
        lsa_interpreter_call_details.interpreterid,
        replace(substring(lsa_interpreter_call_details.incomingtime, 1, 19), 'T', ' ') as incomingtime,
        cast(replace(substring(lsa_interpreter_call_details.servicestarttime, 1, 19), 'T', ' ') as datetime)
            as servicestarttime,
        cast(replace(substring(lsa_interpreter_call_details.serviceendtime, 1, 19), 'T', ' ') as datetime)
            as serviceendtime,
        lsa_interpreter_call_details.serviceduration,
        lsa_interpreter_call_details.charge,
        lsa_interpreter_call_details.connectiontime
    from
        {{source('ods', 'lsa_interpreter_call_details')}} as lsa_interpreter_call_details
),

lsa_data as (
    select
        date_conversion.recordnumber as call_record_number,
        date_conversion.accountcode as account_code,
        date_conversion.caller_phone_number,
        date_conversion.location,
        date_conversion.caller_last_name,
        date_conversion.raw_mrn,
        stg_patient.pat_key,
        stg_patient.mrn,
        date_conversion.requested_language,
        date_conversion.interpreterid as interpreter_id,
        cast(timezone(date_conversion.incomingtime, 'GMT', 'America/New_York') as datetime) as incoming_time,
        cast(timezone(date_conversion.servicestarttime, 'GMT', 'America/New_York') as datetime)
            as service_start_time,
        cast(timezone(date_conversion.serviceendtime, 'GMT', 'America/New_York') as datetime) as service_end_time,
        date(service_start_time) as service_start_date,
        date(service_end_time) as service_end_date,
        date_conversion.serviceduration as service_duration_sec,
        date_conversion.serviceduration / 60 as service_duration_min,
        date_conversion.charge,
        date_conversion.connectiontime as connection_time
    from
        date_conversion as date_conversion
    left join {{ref('stg_patient')}} as stg_patient
        on stg_patient.mrn = date_conversion.raw_mrn
        and stg_patient.current_record_ind = 1 -- bringing current records from stg_patient
),

hospital_ed_encounters as ( --includes hostpial encounters with call between admission and discharge
    select
        'hospital_encounters' as source_1,
        lsa_data.call_record_number,
        stg_encounter.visit_key,
        stg_encounter.hospital_admit_date,
        stg_encounter.hospital_discharge_date
    from
        lsa_data as lsa_data
    inner join {{ref('stg_encounter')}} as stg_encounter
        on lsa_data.pat_key = stg_encounter.pat_key
    where
        lsa_data.service_start_time >= stg_encounter.hospital_admit_date
        and lsa_data.service_start_time <= coalesce(stg_encounter.hospital_discharge_date, current_date)
        and (
            stg_encounter.visit_key in (select visit_key from {{ref('stg_encounter_inpatient')}})
            or stg_encounter.visit_key in (select visit_key from {{ref('stg_encounter_ed')}}
                where ed_patients_seen_ind = 1)
        )
),

single_day_encounters as (
    select
        'single_day_encounters' as source_2,
        lsa_data.call_record_number,
        stg_encounter.visit_key,
        stg_encounter.encounter_date,
        row_number()over(partition by lsa_data.call_record_number
                    order by stg_encounter.appointment_date, stg_encounter.visit_key) as line
        --ex: call_record_number = 22101706785 // i have no idea which visit key should be chosen
        -- for that one because they all have the same start time
    from
        lsa_data as lsa_data
    inner join {{ref('stg_encounter')}} as stg_encounter
        on lsa_data.pat_key = stg_encounter.pat_key
    where
        lsa_data.service_start_date = stg_encounter.encounter_date
        and (stg_encounter.encounter_type_id in (3, --hospital encounter
                                 70, --telephone encounter
                                101, -- office visit
                                 50) -- appointment
           or stg_encounter.appointment_status_id in('1', '2', '6') --scheduled, compelted, arrived;
           )
        and (stg_encounter.appointment_date is null
            or stg_encounter.appointment_date < lsa_data.service_start_time)
            --if appointment time is populated, check that appointment started before call
        and stg_encounter.department_id != '1015002' --interpreter department 
)

select
    lsa_data.call_record_number,
    lsa_data.account_code,
    lsa_data.caller_phone_number,
    lsa_data.location,
    lsa_data.caller_last_name,
    lsa_data.mrn,
    lsa_data.raw_mrn,
    lsa_data.requested_language,
    lsa_data.interpreter_id,
    lsa_data.incoming_time,
    lsa_data.service_start_time,
    lsa_data.service_end_time,
    lsa_data.service_duration_sec,
    lsa_data.service_duration_min,
    lsa_data.charge,
    lsa_data.connection_time,
    lsa_data.pat_key,
    coalesce(single_day_encounters.visit_key, hospital_ed_encounters.visit_key) as visit_key,
    hospital_ed_encounters.visit_key as hopsital_ed_visit_key,
    hospital_ed_encounters.hospital_admit_date,
    hospital_ed_encounters.hospital_discharge_date,
    single_day_encounters.visit_key as single_day_visit_key,
    single_day_encounters.encounter_date
from lsa_data as lsa_data
left join hospital_ed_encounters as hospital_ed_encounters
    on lsa_data.call_record_number = hospital_ed_encounters.call_record_number
left join single_day_encounters as single_day_encounters
    on lsa_data.call_record_number = single_day_encounters.call_record_number and line = 1
