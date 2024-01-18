with all_adt_services as (
    select
        visit_event.visit_key,
        visit_event.visit_event_key,
        coalesce(visit_event.eff_event_dt, visit_event.real_event_dt) as event_datetime,
        pat_service.dict_nm as service,
        -- src_id sorts desc to get order of Transfer Out > Transfer In > Discharge
        -- for events with same eff_event_dt
        case
            when lag(service) over (partition by visit_event.visit_key
                order by event_datetime, adt_event.src_id desc) = service
            then 1
            else 0
        end as same_service_ind,
        row_number() over (
            partition by visit_event.visit_key
            order by event_datetime, adt_event.src_id desc
        ) as row_num
    from
        {{source('cdw','visit_event')}} as visit_event
        inner join {{source('cdw','cdw_dictionary')}} as pat_service
            on pat_service.dict_key = visit_event.dict_pat_svc_key
        inner join {{source('cdw','cdw_dictionary')}} as adt_event
            on visit_event.dict_adt_event_key = adt_event.dict_key
        inner join {{source('cdw','cdw_dictionary')}} as event_subtype
            on visit_event.dict_event_subtype_key = event_subtype.dict_key
    where 
        adt_event.src_id in (1 , 3, 5, 7) -- Admission, Transfer In, Discharge, Patient Update
        and event_subtype.src_id != 2 --Canceled
),

adt_service as (
    select
        all_adt_services.visit_event_key,
        all_adt_services.visit_key,
        all_adt_services.service,
        event_datetime as service_start_datetime,
        lead(event_datetime) over (partition by visit_key order by row_num) as next_service_start_datetime,
        row_num
    from
        all_adt_services
    where
        all_adt_services.same_service_ind = 0
)

select
    adt_service.visit_event_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.hospital_admit_date,
    stg_encounter.hospital_discharge_date,
    adt_service.service,
    adt_service.service_start_datetime,
    coalesce(
        adt_service.next_service_start_datetime,
        stg_encounter.hospital_discharge_date
    ) as service_end_datetime,
    row_number() over (
            partition by adt_service.visit_key
            order by adt_service.row_num
    ) as service_number,
    {{
        dbt_chop_utils.datetime_diff(
            from_date='adt_service.service_start_datetime',
            to_date='service_end_datetime',
            unit='hour'
        )
    }} as service_los_hrs,
    {{
        dbt_chop_utils.datetime_diff(
            from_date='adt_service.service_start_datetime',
            to_date='service_end_datetime',
            unit='day'
        )
    }} as service_los_days,
    stg_encounter.visit_key,
    stg_encounter.encounter_key,
    stg_patient.pat_key,
    stg_patient.patient_key,
    stg_patient.pat_id
from
    adt_service
    inner join {{ ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key =  adt_service.visit_key
    inner join {{ ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = stg_encounter.pat_key
where 
    -- Patient never really on service or Outpatient encounter
    -- and service doesn't happend between admission and discharge dates
    adt_service.service_start_datetime < service_end_datetime
    or service_end_datetime is null
