select
    stg_encounter_outpatient.visit_key,
    stg_encounter_outpatient.patient_name,
    stg_encounter_outpatient.mrn,
    stg_encounter_outpatient.csn,
    stg_encounter_outpatient.encounter_date,
    stg_encounter_outpatient.sex,
    stg_patient.race_ethnicity,
    stg_encounter_outpatient.dob,
    stg_encounter_outpatient.age_years,
    stg_encounter_outpatient.age_days,
    stg_encounter_outpatient.provider_name,
    stg_encounter_outpatient.provider_id,
    provider.title as provider_title,
    case
        when lower(provider.title) in ('crnp', 'pa', 'pa-c') then 1
        when lower(provider.user_id) in ('beeglen', 'schenknj')
            and stg_encounter_outpatient.encounter_date < '2019-02-22' then 1
        else 0
    end as app_is_primary_ind,
    app_is_primary_ind as app_ind,
    case
        when lower(stg_encounter_outpatient.department_name) like 'abington%' then 'Abington'
        when lower(stg_encounter_outpatient.department_name) like 'atl%' then 'Atlantic'
        when lower(stg_encounter_outpatient.department_name) like 'bgr cast%' then 'Buerger - Cast Room'
        when lower(stg_encounter_outpatient.department_name) like 'bgr%' then 'Buerger'
        when lower(stg_encounter_outpatient.department_name) like 'buc%' then 'Bucks'
        when lower(stg_encounter_outpatient.department_name) like 'bwv%' then 'Brandywine'
        when lower(stg_encounter_outpatient.department_name) like 'chadds ford%' then 'Chadds Ford'
        when lower(stg_encounter_outpatient.department_name) like 'curtis%' then 'Curtis'
        when lower(stg_encounter_outpatient.department_name) like 'ext%' then 'Exton'
        when lower(stg_encounter_outpatient.department_name) like 'hup%' then 'HUP'
        when lower(stg_encounter_outpatient.department_name) like 'kop%' then 'KOP'
        when lower(stg_encounter_outpatient.department_name) like 'lgh%' then 'Lancaster'
        when lower(stg_encounter_outpatient.department_name) like 'lankenau%' then 'Lankenau'
        when lower(stg_encounter_outpatient.department_name) like 'mkt 3550%' then 'Market Street'
        when lower(stg_encounter_outpatient.department_name) like 'pnj%' then 'Princeton'
        when lower(stg_encounter_outpatient.department_name) like 'spr%' then 'Springfield'
        when lower(stg_encounter_outpatient.department_name) like 'temple%' then 'Temple'
        when lower(stg_encounter_outpatient.department_name) like 'virtua%' then 'Virtua'
        when lower(stg_encounter_outpatient.department_name) like 'vnj%' then 'Voorhees'
        when lower(stg_encounter_outpatient.department_name) like 'vpf%' then 'Voorhees'
        when lower(stg_encounter_outpatient.department_name) like 'wood cast%' then 'Wood - Cast Room'
        when lower(stg_encounter_outpatient.department_name) like 'wood%' then 'Wood'
        when lower(stg_encounter_outpatient.department_name) like 'main%' then 'Main'
        when lower(stg_encounter_outpatient.department_name) like 'inp%' then 'Main'
        when lower(stg_encounter_outpatient.department_name) like 'pb main%' then 'Main'
    end as department_location,
    stg_encounter_outpatient.department_name,
    stg_encounter_outpatient.department_id,
    department.specialty,
    stg_encounter_outpatient.payor_group,
    stg_encounter_outpatient.encounter_type,
    stg_encounter_outpatient.encounter_type_id,
    stg_encounter_outpatient.appointment_status,
    stg_encounter_outpatient.appointment_status_id,
    stg_encounter_outpatient.appointment_date,
    round(
        hour(stg_encounter_outpatient.appointment_date) + minute(stg_encounter_outpatient.appointment_date) / 60.0,
        2
    ) as scheduled_appointment_time_of_day,
    case
        when visit_appointment_change.visit_appt_chg_dt is null
        then date(visit.appt_made_dt)
        when visit_appointment_change.visit_appt_chg_dt < date(visit.appt_made_dt)
        then visit_appointment_change.visit_appt_chg_dt
        else date(visit.appt_made_dt)
    end as appointment_made_date,
    stg_appointment_note_text.appointment_note_text,
    stg_encounter_outpatient.visit_type,
    stg_encounter_outpatient.visit_type_id,
    initcap(referral_source.full_nm) as referring_provider_name,
    stg_patient_pcp_attribution.pcp_location as primary_care_location,
    year(add_months(stg_encounter_outpatient.encounter_date, 6)) as fiscal_year,
    year(stg_encounter_outpatient.encounter_date) as calendar_year,
    date_trunc('month', stg_encounter_outpatient.encounter_date) as calendar_month,
    date(stg_encounter_outpatient.appointment_date) - date(appointment_made_date) as days_to_appointment,
    stg_encounter_outpatient.cancel_ind,
    stg_encounter_outpatient.noshow_ind,
    stg_encounter_outpatient.lws_ind, --left without seen
    stg_encounter_outpatient.cancel_noshow_ind,
    stg_encounter_outpatient.cancel_noshow_lws_ind,
    stg_encounter_outpatient.past_appointment_ind,
    visit.appt_cancel_dt as appointment_cancellation_date,
    case
        when dim_visit_cncl_rsn.visit_cncl_rsn_nm is null then 0
        when lower(dim_visit_cncl_rsn.visit_cncl_rsn_nm) in (
               'cancellation by provider',
               'chop cancel - provider unavailable',
               'provider/department request'
            ) then 1
        else 0
    end as provider_canceled_ind,
    dim_visit_cncl_rsn.visit_cncl_rsn_nm as cancellation_reason,
    case
        when lag(stg_encounter_outpatient.encounter_date) over(
            partition by stg_encounter_outpatient.mrn, stg_encounter_outpatient.specialty_name
            order by stg_encounter_outpatient.encounter_date
            ) is null then 1
        else 0
    end as first_time_to_specialty_ind,
    stg_encounter_outpatient.new_patient_3yr_ind as new_to_specialty_3_yr_ind,
    stg_encounter_outpatient.telehealth_ind as video_visit_ind,
    case
        when stg_encounter_outpatient.encounter_date >= stg_encounter_mychop_enrollment.first_login_date
        then  stg_encounter_mychop_enrollment.ever_used_ind
        else 0
    end as mychop_ever_used_ind,
    coalesce(stg_encounter_mychop_enrollment.currently_active_ind, 0) as mychop_curently_active_ind,
    case when (
            (stg_encounter_outpatient.encounter_date
                between stg_encounter_mychop_enrollment.first_login_date
                and stg_encounter_mychop_enrollment.last_login_date
            )
            or (
                stg_encounter_outpatient.encounter_date >= stg_encounter_mychop_enrollment.first_login_date
                and stg_encounter_mychop_enrollment.currently_active_ind = 1
            )
        ) then 1
        else 0
    end as mychop_active_on_encounter_ind,
    stg_encounter_outpatient.patient_address_seq_num,
    stg_encounter_outpatient.patient_address_zip_code,
    stg_encounter_outpatient.dept_key,
    stg_encounter_outpatient.prov_key,
    stg_encounter_outpatient.pat_key
from
    {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = stg_encounter_outpatient.pat_key
    inner join {{source('cdw', 'department')}} as department
        on department.dept_key = stg_encounter_outpatient.dept_key
    inner join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = stg_encounter_outpatient.prov_key
    inner join {{source('cdw', 'visit')}} as visit
        on visit.visit_key = stg_encounter_outpatient.visit_key
    inner join {{source('cdw', 'master_visit_type')}} as master_visit_type
        on master_visit_type.visit_type_key = visit.appt_visit_type_key
    left join {{ref('stg_encounter_mychop_enrollment')}} as stg_encounter_mychop_enrollment
        on  stg_encounter_mychop_enrollment.pat_key = stg_encounter_outpatient.pat_key
    left join {{ref('stg_patient_pcp_attribution')}} as stg_patient_pcp_attribution
        on stg_patient_pcp_attribution.pat_key = visit.pat_key
        and visit.eff_dt between stg_patient_pcp_attribution.start_date and stg_patient_pcp_attribution.end_date
    left join {{source('cdw', 'referral_source')}} as referral_source
        on referral_source.ref_src_key = visit.ref_src_key
    left join {{source('cdw', 'dim_visit_cncl_rsn')}} as dim_visit_cncl_rsn
        on dim_visit_cncl_rsn.dim_visit_cncl_rsn_key = visit.dim_visit_cncl_rsn_key
    left join {{ref('stg_appointment_note_text')}} as stg_appointment_note_text
        on stg_encounter_outpatient.visit_key = stg_appointment_note_text.visit_key
    left join {{source('cdw', 'visit_appointment_change')}} as visit_appointment_change
        on visit_appointment_change.visit_key = stg_encounter_outpatient.visit_key
        and visit_appointment_change.seq_num = 1
where
    stg_encounter_outpatient.encounter_type_id in ('101', '50', '76') -- office visit, appointment, telehealth
