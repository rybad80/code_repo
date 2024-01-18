with echos as (
    select
        source_system_id,
        echo_study_id,
        cast(age(cast(cast(study_date_key as varchar(10)) as date), date(dob)) as varchar(64)) as age_at_echo
    from {{ ref('echo_study') }} as echo_study
    inner join {{ source('cdw', 'patient') }} as patient on echo_study.patient_key = patient.pat_key
    where lower(source_system) = 'syngo'
),

measurements as (
    select
        studyid,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'bsa_haycock_calc' then floatvalue end) as decimal (28, 15)) as bsa_haycock_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'patientsweight' then floatvalue end) as decimal (28, 15)) as patientsweight,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'patientssize' then floatvalue end) as decimal (28, 15)) as patientssize,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'bpsystolic' then integervalue end) as integer) as bpsystolic,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'bpdiastolic' then integervalue end) as integer) as bpdiastolic
    from {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype
       on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    where
        lower(syngo_echo_measurementtype.name) in ('bsa_haycock_calc', 'patientssize', 'patientsweight', 'bpdiastolic', 'bpsystolic')
    group by
        studyid
),

sq_echo_study_demographics as (
    select
        cast(echo_study_id as varchar(25)) as echo_study_id,
        cast(age_at_echo as varchar(30)) as age_at_echo,
        cast(patientssize as numeric(28, 15)) as patient_height_cm,
        cast(patientsweight as numeric(28, 15)) as patient_weight_kg,
        cast(bpsystolic as integer) as bp_systolic,
        cast(bpdiastolic as integer) as bp_diastolic,
        cast(bsa_haycock_calc as numeric(28, 15)) as bsa
    from echos
    left join measurements on echos.source_system_id = measurements.studyid
    where
    (patientssize is not null or patientsweight is not null or bpsystolic is not null or bpdiastolic is not null
        or bsa_haycock_calc is not null)
)

select
    echo_study_id,
    age_at_echo,
    patient_height_cm,
    patient_weight_kg,
    bp_systolic,
    bp_diastolic,
    bsa
from sq_echo_study_demographics
