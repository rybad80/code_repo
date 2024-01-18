with ekg_demographics as (
select
    height_cm as patient_height_cm,
    weight_kg as patient_weight_kg,
    cast(age(date(acquisitiondatetime_dt), date(dob)) as varchar(64)) as age_at_ekg,
    muse_tsttestdemographics.testid || 'Muse' as ekg_study_id
from {{ source('muse_ods', 'muse_tsttestdemographics') }} as muse_tsttestdemographics
inner join {{ ref('ekg_study') }} as ekg_study on muse_tsttestdemographics.testid = ekg_study.source_system_id and lower(ekg_study.source_system) = 'muse'
inner join {{ source('muse_ods', 'muse_tstpatientdemographics') }} as muse_tstpatientdemographics
     on muse_tsttestdemographics.testid = muse_tstpatientdemographics.testid
inner join {{ source('cdw', 'patient') }} as patient on ekg_study.patient_key = patient.pat_key
where
    (height_cm is not null
    or weight_kg is not null
    or age(date(acquisitiondatetime_dt), date(dob)) is not null)
)
select
    cast(ekg_study.ekg_study_id as varchar(25)) as ekg_study_id,
    cast(patient_height_cm as integer) as patient_height_cm,
    cast(patient_weight_kg as integer) as patient_weight_kg,
    cast(age_at_ekg as varchar(30)) as age_at_ekg
from ekg_demographics
inner join {{ ref('ekg_study') }} as ekg_study on ekg_demographics.ekg_study_id = ekg_study.ekg_study_id
