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
    select distinct
        syngo_echo_observationvalue.studyid,
        lower(syngo_echo_obs_meas_mapping.cdwfield) as cdwfield,
        isnull(cast(syngo_echo_observationfieldmap.worksheetvalue as varchar(400)),
            cast(syngo_echo_observationvalue.val as varchar(400))) as displayvalue
    from {{ source('syngo_echo_ods', 'syngo_echo_observationvalue') }} as syngo_echo_observationvalue
    inner join {{ source('syngo_echo_ods', 'syngo_echo_observationname') }} as syngo_echo_observationname
       on syngo_echo_observationvalue.observationid = syngo_echo_observationname.id
    inner join {{ source('syngo_echo_ods', 'syngo_echo_obs_meas_mapping') }} as syngo_echo_obs_meas_mapping on syngo_echo_obs_meas_mapping.databasename = syngo_echo_observationname.name
    left join {{ source('syngo_echo_ods', 'syngo_echo_observationfieldmap') }} as syngo_echo_observationfieldmap
       on syngo_echo_observationfieldmap.observationname = syngo_echo_observationname.name
       and syngo_echo_observationfieldmap.databasevalue = syngo_echo_observationvalue.val
    where lower(syngo_echo_obs_meas_mapping.cdwfield) in
        ('advanced_3d_valves', 'advanced_3d_study', 'chop_location', 'site')
),

observations as (
    select
        studyid,
        case when cdwfield = 'advanced_3d_valves' then displayvalue end as advanced_3d_valves,
        case when cdwfield = 'advanced_3d_study' then displayvalue end as advanced_3d_study,
        case when cdwfield = 'chop_location' then displayvalue end as chop_location,
        case when cdwfield = 'site' then displayvalue end as site
    from observation_display_values
),

observation_group as (
    select
        studyid,
        group_concat(advanced_3d_valves, ';') as advanced_3d_valves,
        max(advanced_3d_study) as advanced_3d_study,
        max(chop_location) as chop_location,
        max(site) as site
    from observations
    group by studyid
),

sq_echo_study_data as (
select
        syngo_echo_dosr_study.study_date as study_date_key,
        syngo_echo_dosr_study.study_ref || 'Syn' as echo_study_id,
        strleft(syngo_echo_dosr_study.study_time, 6) as study_time,
        patient.pat_key as patient_key,
        syngo_echo_dosr_study.study_ref as source_system_id,
        'Syngo' as source_system,
        syngo_echo_dosr_study.study_description as study_type,
        syngo_echo_parse_dicom_names.operator_name as sonographer,
        syngo_echo_parse_dicom_names.physician_reading_study as attending,
        syngo_echo_parse_dicom_names.name_of_fellow_reading_study as fellow,
        cast(replace(replace(replace(replace(replace(replace(replace(interpretdiagnosisdescription, chr(9), ' '), chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '), chr(124), '') as varchar(2000)) as diagnosis_description,
        cast(replace(replace(replace(replace(replace(replace(replace(indication, chr(9), ' '), chr(10), ' '), chr(11), ' '), chr(12), ' '), chr(13), ' '), chr(14), ' '), chr(124), '') as varchar(2000)) as indication,
        syngo_echo_dosr_study.num_images as number_of_images,
        syngo_echo_dosr_study.institutional_department_name as department_name,
        observation_group.advanced_3d_valves,
        observation_group.advanced_3d_study,
        observation_group.site,
        observation_group.chop_location
    from {{ source('syngo_echo_ods', 'syngo_echo_dosr_study') }} as syngo_echo_dosr_study
    inner join {{ source('syngo_echo_ods', 'syngo_echo_study') }} as syngo_echo_study on syngo_echo_dosr_study.study_ref = syngo_echo_study.studyid
    left join final_report on syngo_echo_dosr_study.study_ref = final_report.studyid
    inner join {{ source('cdw', 'patient_match') }} as patient_match on syngo_echo_dosr_study.study_ref = patient_match.src_sys_id
        and lower(patient_match.src_sys_nm) = 'syngo_echo'
    inner join {{ source('cdw', 'patient') }} as patient on patient_match.pat_key = patient.pat_key
    left join {{ source('syngo_echo_ods', 'syngo_echo_parse_dicom_names') }} as syngo_echo_parse_dicom_names on syngo_echo_study.studyid = syngo_echo_parse_dicom_names.study_id
    left join observation_group on syngo_echo_study.studyid = observation_group.studyid
where
    lower(syngo_echo_dosr_study.institutional_department_name) in ('pedecho', 'extramural_studies')
        and final_report.syngoflag >= 1-- report is final
)

select
    cast(echo_study_id as varchar(25)) as echo_study_id,
    cast(study_date_key as integer) as study_date_key,
    cast(study_time as varchar(8)) as study_time,
    cast(patient_key as bigint) as patient_key,
    cast(source_system_id as integer) as source_system_id,
    cast(source_system as varchar(20)) as source_system,
    cast(study_type as varchar(64)) as study_type,
    cast(diagnosis_description as varchar(2000)) as diagnosis_description,
    cast(indication as varchar(2000)) as indication,
    cast(fellow as varchar(200)) as fellow,
    cast(sonographer as varchar(200)) as sonographer,
    cast(attending as varchar(200)) as attending,
    cast(advanced_3d_valves as varchar(255)) as advanced_3d_valves,
    cast(advanced_3d_study as varchar(255)) as advanced_3d_study,
    cast(number_of_images as integer) as number_of_images,
    cast(department_name as varchar(64)) as department_name,
    cast(site as varchar(64)) as site,
    cast(chop_location as varchar(64)) as chop_location
from sq_echo_study_data
