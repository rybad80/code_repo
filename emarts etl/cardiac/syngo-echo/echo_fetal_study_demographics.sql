with fetal_echos as (
    select
        source_system_id,
        echo_fetal_study_id,
        cast(age(cast(cast(study_date_key as varchar(10)) as date), date(dob)) as varchar(64)) as age_at_echo
        from {{ ref('echo_fetal_study') }} as echo_fetal_study
    inner join {{ source('cdw', 'patient') }} as patient on echo_fetal_study.patient_key = patient.pat_key
    where lower(source_system) = 'syngo'
),

measurements as (
    select
        studyid,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'bsa_boyd_calc' then floatvalue end) as decimal (28, 15)) as bsa_boyd_calc,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'patientsweight' then floatvalue end) as decimal (28, 15)) as patientsweight,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'patientssize' then floatvalue end) as decimal (28, 15)) as patientssize,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'bpsystolic' then integervalue end) as integer) as bpsystolic,
        cast(avg(case when lower(syngo_echo_measurementtype.name) = 'bpdiastolic' then integervalue end) as integer) as bpdiastolic
    from {{ source('syngo_echo_ods', 'syngo_echo_measurementvalue') }} as syngo_echo_measurementvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_measurementtype') }} as syngo_echo_measurementtype
        on syngo_echo_measurementvalue.measurementtypeidx = syngo_echo_measurementtype.id
    where
        lower(syngo_echo_measurementtype.name) in ('bsa_boyd_calc', 'patientssize', 'patientsweight', 'bpdiastolic', 'bpsystolic')
    group by
        studyid
),

sq_echo_fetal_study_demographics as (
    select
        fetal_echos.echo_fetal_study_id,
        age_at_echo,
        patientssize as patient_height_cm,
        patientsweight as patient_weight_kg,
        bpsystolic as bp_systolic,
        bpdiastolic as bp_diastolic,
        bsa_boyd_calc as bsa
    from fetal_echos
    left join measurements on fetal_echos.source_system_id = measurements.studyid
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(age_at_echo as varchar(30)) as age_at_echo,
    patient_height_cm as patient_height_cm,
    patient_weight_kg as patient_weight_kg,
    cast(bp_systolic as integer) as bp_systolic,
    cast(bp_diastolic as integer) as bp_diastolic,
    bsa as bsa
from sq_echo_fetal_study_demographics
