/* Vitals data from Moberg Devices */

{{
    config(materialized = 'view')
}}

with moberg_vitals as (
    select
        vitals.mrn,
        vitals.vital_sign,
        cast(vitals.vital_sign_value as numeric) as vital_sign_value,
        patient.patient_name,
        patient.dob,
        cast(patient.dob as date)
            + cast(vitals.day_recorded || ' days' as interval)
            + cast(vitals.clocktime as time)
            + cast(vitals.msec || ' millisecond' as interval) as vital_sign_datetime
    from {{ source('manual_ods_cfdt', 'moberg_vitals') }} as vitals
    left join {{ ref('stg_patient') }} as patient on vitals.mrn = patient.mrn
)

select
    mrn,
    patient_name,
    dob,
    vital_sign_datetime,
    max(case when lower(vital_sign) = 'hr,na' then vital_sign_value end) as hr,
    max(case when lower(vital_sign) = 'nbp,syst' then vital_sign_value end) as nbp_syst,
    max(case when lower(vital_sign) = 'nbp,dias' then vital_sign_value end) as nbp_dias,
    max(case when lower(vital_sign) = 'nbp,mean' then vital_sign_value end) as nbp_mean,
    max(case when lower(vital_sign) = 'spo2,na' then vital_sign_value end) as spo2,
    max(case when lower(vital_sign) = 'rr,na' then vital_sign_value end) as rr,
    max(case when lower(vital_sign) = 'rso2,left' then vital_sign_value end) as nirs_rso2_left,
    max(case when lower(vital_sign) = 'rso2,right' then vital_sign_value end) as nirs_rso2_right
from moberg_vitals
group by
    mrn,
    patient_name,
    dob,
    vital_sign_datetime
