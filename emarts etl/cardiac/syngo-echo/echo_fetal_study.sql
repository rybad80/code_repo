with final_report as (
    select
        studyid,
        sum(case when lower(reportstate) = 'verified'
            and isnull(lower(watermark), '') != 'preliminary' then 1
        else 0 end) as syngoflag
from {{ source('syngo_echo_ods', 'syngo_echo_report') }}
group by studyid
),

observation_display_values as (
    select
        syngo_echo_observationvalue.studyid,
        lower(syngo_echo_observationname.name) as observation_name,
        isnull(cast(syngo_echo_fetalobservationfieldmap.worksheetvalue as varchar(400)),
            cast(syngo_echo_observationvalue.val as varchar(400))) as displayvalue
    from {{ source('syngo_echo_ods', 'syngo_echo_observationvalue') }} as syngo_echo_observationvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_observationname') }} as syngo_echo_observationname
        on syngo_echo_observationvalue.observationid = syngo_echo_observationname.id
    left join {{ source('syngo_echo_ods', 'syngo_echo_fetalobservationfieldmap') }} as syngo_echo_fetalobservationfieldmap
        on syngo_echo_fetalobservationfieldmap.observationname = syngo_echo_observationname.name
        and syngo_echo_fetalobservationfieldmap.databasevalue = syngo_echo_observationvalue.val
    where lower(syngo_echo_observationname.name) in
        ('imaging_diagnosis_obs', 'patient_location_edited_1_obs', 'chop_site_obs', 'chop_proc_codes_0_obs')
),

observations as (
    select
        studyid,
        case when observation_name = 'imaging_diagnosis_obs' then displayvalue end as imaging_diagnosis_obs,
        case when observation_name = 'patient_location_edited_1_obs' then displayvalue end as patient_location_edited_1_obs,
        case when observation_name = 'chop_site_obs' then displayvalue end as chop_site_obs,
        case when observation_name = 'chop_proc_codes_0_obs' then displayvalue end as chop_proc_codes_0_obs
    from observation_display_values
),

observation_group as (
    select
        studyid,
        group_concat(imaging_diagnosis_obs, ';') as imaging_diagnosis_obs,
        group_concat(patient_location_edited_1_obs, ';') as patient_location_edited_1_obs,
        group_concat(chop_site_obs, ';') as chop_site_obs,
        group_concat(chop_proc_codes_0_obs, ';') as chop_proc_codes_0_obs
    from observations
    group by studyid
),

sq_echo_fetal_study as (
    select
        syngo_echo_dosr_study.study_date as study_date_key,
        syngo_echo_dosr_study.study_ref || 'Syn' as echo_fetal_study_id,
        strleft(syngo_echo_dosr_study.study_time, 6) as study_time,
        patient.pat_key as patient_key,
        syngo_echo_dosr_study.study_ref as source_system_id,
        'Syngo' as source_system,
        syngo_echo_dosr_study.study_description as study_type,
        replace(replace(replace(replace(replace(replace(replace(cast(interpretdiagnosisdescription as varchar(2000)),
            chr(9), ' '), chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '),
            chr(124), '') as diagnosis_description,
        replace(replace(replace(replace(replace(replace(replace(cast(indication as varchar(2000)), chr(9), ' '),
            chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '), chr(124), '') as indication,
        chop_proc_codes_0_obs as procedure,
        imaging_diagnosis_obs as imaging_diagnosis,
        syngo_echo_parse_dicom_names.operator_name as sonographer,
        syngo_echo_parse_dicom_names.physician_reading_study as attending,
        syngo_echo_parse_dicom_names.referring_physician,
        syngo_echo_dosr_study.institutional_department_name as department_name,
        chop_site_obs as site,
        patient_location_edited_1_obs as chop_location
    from {{ source('syngo_echo_ods', 'syngo_echo_dosr_study') }} as syngo_echo_dosr_study
    left join {{ source('syngo_echo_ods', 'syngo_echo_study') }} as syngo_echo_study
        on syngo_echo_dosr_study.study_ref = syngo_echo_study.studyid
    left join final_report on syngo_echo_dosr_study.study_ref = final_report.studyid
    inner join {{ source('cdw', 'patient_match') }} as patient_match
        on syngo_echo_dosr_study.study_ref = patient_match.src_sys_id
        and lower(patient_match.src_sys_nm) = 'syngo_echo'
    left join {{ source('cdw', 'patient') }} as patient on patient_match.pat_key = patient.pat_key
    left join {{ source('syngo_echo_ods', 'syngo_echo_parse_dicom_names') }} as syngo_echo_parse_dicom_names
        on syngo_echo_study.studyid = syngo_echo_parse_dicom_names.study_id
    left join observation_group
        on syngo_echo_dosr_study.study_ref = observation_group.studyid
    where
        lower(syngo_echo_dosr_study.institutional_department_name) in ('fetalecho', 'mfm')
    and final_report.syngoflag >= 1-- report is final
)

select
    cast(echo_fetal_study_id as varchar(25)) as echo_fetal_study_id,
    cast(study_date_key as integer) as study_date_key,
    cast(study_time as varchar(16)) as study_time,
    cast(patient_key as bigint) as patient_key,
    cast(source_system_id as integer) as source_system_id,
    cast(source_system as varchar(20)) as source_system,
    cast(study_type as varchar(64)) as study_type,
    cast(diagnosis_description as varchar(2000)) as diagnosis_description,
    cast(indication as varchar(2000)) as indication,
    cast(procedure as varchar(255)) as procedure,
    cast(imaging_diagnosis as varchar(255)) as imaging_diagnosis,
    cast(sonographer as varchar(200)) as sonographer,
    cast(attending as varchar(200)) as attending,
    cast(referring_physician as varchar(200)) as referring_physician,
    cast(department_name as varchar(64)) as department_name,
    cast(site as varchar(64)) as site,
    cast(chop_location as varchar(64)) as chop_location
from sq_echo_fetal_study
