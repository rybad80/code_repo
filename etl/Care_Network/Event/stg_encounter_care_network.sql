{{ config(materialized='table', dist='csn') }}

select
    stg_encounter.visit_key,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.pat_key,
    stg_encounter.pat_id,
    stg_encounter.mrn,
    stg_encounter.patient_name,
    initcap(provider.full_nm) as provider_name,
    provider.prov_id as provider_id,
    stg_encounter.prov_key,
    stg_encounter.dept_key,
    stg_encounter.department_name,
    stg_encounter.department_id,
    stg_encounter.encounter_type_id,
    stg_encounter.encounter_type,
    stg_encounter.appointment_status,
    stg_encounter.appointment_status_id,
    stg_encounter.los_proc_cd as level_service_procedure_code,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    clarity_ser.prov_type,
    case when (lower(clarity_ser.prov_name) not like '%provider%'
        and lower(clarity_ser.prov_name) not like '%nurse%'
        and lower(clarity_ser.prov_name) not like '%study%'
        and lower(clarity_ser.prov_name) not like '% prov'
        and lower(clarity_ser.prov_name) not like '% room'
        and lower(clarity_ser.prov_name) not like '% program'
        and lower(clarity_ser.prov_name) not like '% clinic%'
        and lower(clarity_ser.prov_name) not like '% shot'
        and lower(clarity_ser.prov_name) not like '% lab'
        and lower(clarity_ser.prov_name) not like 'transplant%'
        and lower(clarity_ser.prov_name) not like '% other'
        ) then 1
    else 0 end as provider_ind
    from
    {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
    on provider.prov_key = stg_encounter.prov_key
    inner join {{ source('clarity_ods', 'clarity_ser') }} as clarity_ser
    on clarity_ser.prov_id = provider.prov_id
    where
    stg_encounter.encounter_type_id in (62, -- evisit keep
                                    204, -- sunday office visit
                                    76 -- telemedicine
                                    )
    or (encounter_type_id in (101, 50) -- office visit, appointment
    and appointment_status_id in (-2, -- not applicable
                                    2, -- completed
                                    6 -- arrived
                                    ))
    and lower(clarity_ser.prov_type) != 'resource'
