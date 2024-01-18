select
    stg_encounter.visit_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.pat_id,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.sex,
    stg_encounter.age_years,
    stg_encounter.age_days,
    initcap(provider.full_nm) as provider_name,
    provider.prov_id as provider_id,
    provider.prov_type as provider_type,
    stg_department_all.department_name,
    cast(stg_department_all.department_id as bigint) as department_id,
    stg_department_all.department_center_abbr,
    stg_department_all.department_center_id,
    stg_encounter.patient_address_seq_num,
    stg_encounter.patient_address_zip_code,
    stg_encounter.county,
    stg_encounter.state,
    stg_encounter_chop_market.chop_market,
    stg_encounter_chop_market.region_category,
    stg_encounter_payor.payor_name,
    stg_encounter_payor.payor_group,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    stg_encounter.appointment_status,
    stg_encounter.appointment_status_id,
    stg_encounter.patient_class,
    stg_encounter.patient_class_id,
    service_area.svc_area_nm as service_area,
    cast(service_area.svc_area_id as integer) as service_area_id,
    stg_encounter.hospital_admit_date,
    stg_encounter.hospital_discharge_date,
    stg_encounter.appointment_date,
    stg_encounter.original_appointment_made_date,
    stg_encounter.begin_checkin_date,
    stg_encounter.echeckin_status_name,
    stg_encounter.original_appointment_made_user_id,
    employee.emp_key as original_appointment_made_clarity_emp_key,
    initcap(stg_encounter.secondary_provider_name) as secondary_provider_name,
    initcap(stg_encounter.secondary_provider_type) as secondary_provider_type,
    stg_encounter.los_proc_cd as level_service_procedure_code,
    case when stg_encounter.los_proc_cd like '993%' then 1 else 0 end as well_visit_ind,
    -- Cancelled, no-show, left without seen appointment status
    stg_encounter.cancel_ind,
    stg_encounter.noshow_ind,
    stg_encounter.lws_ind, --left without seen
    stg_encounter.cancel_noshow_ind,
    stg_encounter.cancel_noshow_lws_ind,
    coalesce(stg_encounter_outpatient.primary_care_ind, 0) as primary_care_ind,
    coalesce(stg_encounter_outpatient.specialty_care_ind, 0) as specialty_care_ind,
    coalesce(stg_encounter_outpatient.physician_app_psych_visit_ind, 0) as physician_app_psych_visit_ind,
    coalesce(stg_encounter_outpatient.urgent_care_ind, 0) as urgent_care_ind,
    case when stg_encounter_inpatient.visit_key is not null then 1 else 0 end as inpatient_ind,
    case when stg_encounter_ed.encounter_key is not null then 1 else 0 end as ed_ind,
    case when stg_encounter_telehealth.visit_key is not null then 1 else 0 end as telehealth_ind,
    stg_encounter.walkin_ind,
    stg_encounter_outpatient.phys_app_psych_online_scheduled_ind,
    stg_encounter.online_scheduled_ind,
    coalesce(stg_encounter_outpatient.mychop_scheduled_ind, 0) as mychop_scheduled_ind,
    coalesce(stg_encounter_gps.global_patient_services_ind, 0) as global_patient_services_ind,
    case
        when stg_encounter_chop_market.chop_market = 'international'
        then 1 else 0
    end as international_ind,
    stg_encounter.echeckin_complete_ind,
    -- Need to add in Surgery block indicator
    -- Need to add in Virtual block indicator?
    -- These are uppercase and in quotes because this is how FACT_READMISSIONS table uses the columns
    -- Due to column names leading with a number
    -- add to bubble exclusion: encounter_type_description
    -- Reach out matt dye regarding re-admissions data... are all departments included in this list
    -- , coalesce(fact_readmissions."7DAY_READM_IND", 0) as readmission_7day_ind
    -- , coalesce(fact_readmissions."14DAY_READM_IND", 0) as readmission_14day_ind
    -- , coalesce(fact_readmissions."30DAY_READM_IND", 0) as readmission_30day_ind
    -- , coalesce(fact_readmissions."90DAY_READM_IND", 0) as readmission_90day_ind
    stg_encounter.dept_key,
    provider.prov_key,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    stg_encounter.encounter_key,
    stg_encounter.patient_key,
    stg_encounter.provider_key
from
    {{ref('stg_encounter')}} as stg_encounter
    inner join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    inner join {{ref('stg_department_all')}} as stg_department_all
        on stg_department_all.dept_key = stg_encounter.dept_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter_payor.visit_key = stg_encounter.visit_key
    left join {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
        on stg_encounter_outpatient.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
        on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
    left join {{ref('stg_encounter_ed')}} as stg_encounter_ed
        on stg_encounter_ed.encounter_key = stg_encounter.encounter_key
        and stg_encounter_ed.ed_patients_seen_ind = 1
    -- Need to add in Surgery block
    -- Need to add in Virtual block
    -- left join ocqi_uat..fact_readmissions on fact_readmissions.visit_key = visit.visit_key
    left join {{source('cdw', 'service_area')}} as service_area
        on service_area.svc_area_key = stg_encounter.svc_area_key
    left join {{ ref('stg_encounter_telehealth') }} as stg_encounter_telehealth
        on stg_encounter_telehealth.visit_key = stg_encounter.visit_key
    left join {{ ref('stg_encounter_gps') }} as stg_encounter_gps
        on stg_encounter_gps.encounter_key = stg_encounter.encounter_key
    left join {{ ref('stg_encounter_chop_market')}} as stg_encounter_chop_market
        on stg_encounter_chop_market.encounter_key = stg_encounter.encounter_key
    left join {{source('cdw', 'employee')}} as employee
        on stg_encounter.original_appointment_made_user_id = employee.emp_id and employee.comp_key = 1
