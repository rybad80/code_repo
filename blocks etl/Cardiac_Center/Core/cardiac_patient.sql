with cardiac_patients as (
    select
        pat_key
    from
        {{ref('cardiac_study')}}

    union all

    select
        pat_key
    from
        {{ref('stg_encounter')}}
    where
        lower(department_name) like '%cardiology%'
        or lower(department_name) like '%cardiac%'
        or lower(department_name) like '%heart%'
),

stg_cardiac_patient_cohort_lymphatics as (
    select distinct
        pat_key,
        1 as lymphatics_ind
 from
      {{source('clarity_ods', 'patient_fyi_flags')}}  as patient_fyi_flags
      inner join {{source('cdw', 'patient')}} as patient on patient_fyi_flags.patient_id = patient.pat_id
where
     pat_flag_type_c = '1099'
     and active_c = 1
),

stg_cardiac_patient_cohort_ph as (
select distinct
      stg_encounter.pat_key,
      1 as ph_ind
  from
      {{ref('stg_encounter')}} as stg_encounter
      inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
where
      stg_encounter.encounter_date between date(current_timestamp) - (365 * 5)
        and date(current_timestamp)
      and provider.prov_id in ('12166', '4602', '602298', '9376', '9622', '10712', '4329', '9001065')
      and stg_encounter.visit_type_id in ('8912', '1701', '8921', '8922')--,'2124','2088')
      and stg_encounter.encounter_type_id in ('101', '50')
      and stg_encounter.appointment_status_id in (2, 6, -2)
      and stg_encounter.patient_class_id in ('2', '6', '0')
),

stg_cardiac_patient_isvmp as (
select distinct
      pat_key,
      1 as isvmp_ind
  from
      {{source('cdw', 'patient_list_info')}} as patient_list_info
      inner join {{source('cdw', 'patient_list')}} as patient_list
        on patient_list.pat_lst_info_key = patient_list_info.pat_lst_info_key
where
    patient_list_info.pat_lst_info_id in (
        45728,
        422037,
        83822,
        237463,
        238281,
        237466,
        237462,
        200433,
        135257,
        420076,
        72064,
        237465,
        110763)
    and patient_list.cur_rec_ind = 1
)

select distinct
    patient_all.pat_key,
    patient.pat_id,
    patient_all.patient_name,
    patient_all.mrn,
    patient_all.dob,
    patient_all.sex,
    patient_all.current_age,
    patient_all.email_address,
    patient_all.mailing_address_line1,
    patient_all.mailing_address_line2,
    patient_all.mailing_city,
    patient_all.mailing_state,
    patient_all.mailing_zip,
    patient_all.county,
    patient_all.gestational_age_complete_weeks,
    patient_all.gestational_age_remainder_days,
    patient_all.birth_weight_kg,
    patient_all.race,
    patient_all.ethnicity,
    patient_all.race_ethnicity,
    patient_all.preferred_language,
    patient_all.texting_opt_in_ind,
    patient_all.deceased_ind,
    patient_all.current_record_ind,
    patient_all.payor_name,
    patient_all.payor_group,
    patient_all.payor_start_date,
    patient_all.current_pcp_location,
    patient_all.mychop_activation_ind,
    patient_all.mychop_declined_ind,
    coalesce(stg_cardiac_patient_cohort_ph.ph_ind, 0) as ph_ind,
    coalesce(stg_cardiac_patient_cohort_lymphatics.lymphatics_ind, 0) as lymphatics_ind,
    coalesce(stg_cardiac_patient_isvmp.isvmp_ind, 0) as isvmp_ind

from
    {{ref('patient_all')}} as patient_all
    inner join {{source('cdw', 'patient')}} as patient on patient.pat_key = patient_all.pat_key
    inner join cardiac_patients on patient_all.pat_key = cardiac_patients.pat_key
    left join stg_cardiac_patient_cohort_ph on stg_cardiac_patient_cohort_ph.pat_key = cardiac_patients.pat_key
    left join stg_cardiac_patient_cohort_lymphatics
        on stg_cardiac_patient_cohort_lymphatics.pat_key = cardiac_patients.pat_key
    left join stg_cardiac_patient_isvmp on stg_cardiac_patient_isvmp.pat_key = cardiac_patients.pat_key
