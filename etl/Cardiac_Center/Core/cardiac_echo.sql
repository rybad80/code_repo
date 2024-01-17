with ped as (
     select
          echo_study.echo_study_id as echo_study_id,
          echo_study.patient_key as pat_key,
          to_date(echo_study.study_date_key, 'YYYYMMDD') as study_date,
          1 as ped_ind,
          0 as fetal_ind,
          echo_study.site,
          echo_study.chop_location,
          echo_study.department_name,
          echo_study.study_time,
          echo_study.study_type,
          echo_study.source_system_id,
          row_number() over (
               partition by echo_study.patient_key, echo_study.study_date_key
               order by echo_study.study_time
          ) as echo_study_order
     from
          {{source('cdw', 'echo_study')}} as echo_study
),
fetal as (
     select
          echo_fetal_study.echo_fetal_study_id as echo_study_id,
          echo_fetal_study.patient_key as pat_key,
          to_date(echo_fetal_study.study_date_key, 'YYYYMMDD') as study_date,
          0 as ped_ind,
          1 as fetal_ind,
          echo_fetal_study.site,
          echo_fetal_study.chop_location,
          echo_fetal_study.department_name,
          echo_fetal_study.study_time,
          echo_fetal_study.study_type,
          echo_fetal_study.source_system_id,
          row_number() over (
               partition by echo_fetal_study.patient_key, echo_fetal_study.study_date_key
               order by echo_fetal_study.study_time
          ) as echo_study_order
     from
          {{source('cdw', 'echo_fetal_study')}} as echo_fetal_study
),
echo_all as (
     select
          echo_study_id,
          pat_key,
          study_date,
          ped_ind,
          fetal_ind,
          site,
          chop_location,
          department_name,
          study_type,
          echo_study_order,
          source_system_id
     from
          ped
     union all
     select
          echo_study_id,
          pat_key,
          study_date,
          ped_ind,
          fetal_ind,
          site,
          chop_location,
          department_name,
          study_type,
          echo_study_order,
          source_system_id
     from
          fetal
),
echo_encounters as (
     select
          pat_key,
          visit_key,
          visit_type,
          date(encounter_date) as study_date,
          time(appointment_date) as study_time,
          row_number() over (
               partition by pat_key, date(encounter_date)
               order by time(appointment_date)
          ) as echo_study_order
     from
          {{ref('stg_encounter')}}
     where
          lower(visit_type) like '%echo%'
          and appointment_status = 'COMPLETED'
),
encounter_match as (
     select
          source_system_id,
          echo_encounters.visit_key,
          echo_encounters.pat_key
     from
          echo_all
          left join echo_encounters
               on echo_encounters.pat_key = echo_all.pat_key
               and echo_encounters.study_date = echo_all.study_date
               and echo_encounters.echo_study_order = echo_all.echo_study_order
),
echo_order as (
    select
          order_rad_acc_num.order_proc_id,
          order_rad_acc_num.acc_num
      from
           {{source('clarity_ods', 'order_rad_acc_num')}} as order_rad_acc_num
           inner join {{source('clarity_ods', 'order_proc')}} as order_proc
              on order_rad_acc_num.order_proc_id = order_proc.order_proc_id
    where
        order_type_c = 5007
        and radiology_status_c = 99
),

report as (
select
     studyid,
     creationtime,
     completiontime,
     modificationtime,
     row_number() over (partition by studyid order by creationdate) as created_order
  from
     {{source('ccis_ods', 'syngo_echo_report')}}
where
     reportstate = 'Verified'
     and isnull(watermark, '') != 'Preliminary'
)

     select
          echo_all.echo_study_id as cardiac_study_id,
          echo_all.pat_key,
          echo_order.order_proc_id as procedure_order_id,
          stg_patient.mrn,
          stg_patient.patient_name,
          stg_patient.sex,
          stg_patient.dob,
          coalesce(encounter_match.visit_key, 0) as visit_key,
          echo_all.study_date,
          case when lower(trim(echo_all.study_type)) like '%transthoracic%' then 'Transthoracic'
               when lower(echo_all.study_type) like '%sedated%tte%' then 'Sedated TTE'
               when lower(echo_all.study_type) like '%tte%' then 'Transthoracic'
               when lower(echo_all.study_type) like 'ped%echo%' then 'Transthoracic'
               when lower(echo_all.study_type) = 'echocardiogram' then 'Transthoracic'
               when lower(echo_all.study_type) like '%tee%' then 'Transesophageal'
               when lower(echo_all.study_type) like '%trans%esophageal%' then 'Transesophageal'
               when lower(echo_all.study_type) like '%stress%' then 'Stress Echo'
               when lower(echo_all.study_type) like '%fetal%' then 'Fetal Echo'
               when lower(echo_all.study_type) = 'ob' then 'Fetal Echo'
               when lower(echo_all.study_type) like '%epicardial%' then 'Epicardial'
               when lower(echo_all.study_type) like '%vascular%' then 'Vascular'
            else initcap(echo_all.study_type)
          end as echo_type,
          echo_all.ped_ind,
          echo_all.fetal_ind,
          case when echo_all.site is null then 'Undefined' else site end as site,
          case when echo_all.chop_location is null then 'Undefined' else chop_location end as chop_location,
          echo_all.department_name,
          report.creationtime as report_creation_date,
          report.completiontime as report_completion_date,
          report.modificationtime as report_modification_date,
          cardiac_study.inpatient_ind
     from
          echo_all
          inner join {{ref('cardiac_study')}} as cardiac_study
               on cardiac_study.cardiac_study_id = echo_all.echo_study_id
          inner join {{ref('stg_patient')}} as stg_patient
               on stg_patient.pat_key = echo_all.pat_key
          left join encounter_match
               on echo_all.source_system_id = encounter_match.source_system_id
          left join {{source('ccis_ods', 'syngo_echo_dosr_study')}} as dosr_study
               on dosr_study.study_ref = echo_all.source_system_id
          left join report
               on report.studyid = echo_all.source_system_id
               and report.created_order = 1
          left join echo_order
               on echo_order.acc_num = dosr_study.accession_number
