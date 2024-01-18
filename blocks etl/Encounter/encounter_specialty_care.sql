-- specialty care encounters
select
    stg_encounter_outpatient.visit_key,
    stg_encounter_outpatient.encounter_key,
    stg_encounter_outpatient.patient_name,
    stg_encounter_outpatient.mrn,
    stg_encounter_outpatient.dob,
    stg_encounter_outpatient.csn,
    stg_encounter_outpatient.encounter_date,
    stg_encounter_outpatient.sex,
    stg_encounter_outpatient.age_years,
    stg_encounter_outpatient.age_days,
    stg_encounter_outpatient.provider_name,
    stg_encounter_outpatient.physician_app_psych_visit_ind,
    initcap(referral_source.full_nm) as referring_provider_name,
    stg_encounter_outpatient.department_name,
    stg_encounter_outpatient.department_id,
    stg_encounter_outpatient.specialty_name,
    stg_patient_pcp_attribution.pcp_location as primary_care_location,
    stg_encounter_outpatient.patient_address_seq_num,
    stg_encounter_outpatient.patient_address_zip_code,
    stg_encounter_outpatient.chop_market,
    stg_encounter_outpatient.region_category,
    stg_encounter_outpatient.payor_name,
    stg_encounter_outpatient.payor_group,
    stg_encounter_outpatient.visit_type,
    stg_encounter_outpatient.visit_type_id,
    stg_encounter_outpatient.original_appointment_made_date,
    stg_encounter_outpatient.appointment_made_date,
    stg_encounter_outpatient.appointment_date,
    stg_encounter_outpatient.start_visit_date,
    stg_encounter_outpatient.check_in_date,
    stg_encounter_outpatient.assign_room_date,
    stg_encounter_outpatient.start_rooming_date,
    stg_encounter_outpatient.done_rooming_date,
    stg_encounter_outpatient.check_out_date,
    stg_encounter_outpatient.complete_visit_date,
    stg_encounter_outpatient.encounter_close_date,
    stg_encounter_outpatient.scheduled_length_min,
    stg_encounter_outpatient.actual_length_min,
    stg_encounter_outpatient.scheduled_to_encounter_days,
    stg_encounter_outpatient.npv_appointment_lag_days,
    stg_encounter_outpatient.npv_lag_incl_ind,
    stg_encounter_outpatient.complex_chronic_condition_ind,
    stg_encounter_outpatient.medically_complex_ind,
    stg_encounter_outpatient.tech_dependent_ind,
    stg_encounter_outpatient.physician_service_level_ind,
    stg_encounter_outpatient.appointment_ind,
    stg_encounter_outpatient.office_visit_ind,
    stg_encounter_outpatient.recurring_outpatient_ind,
    stg_encounter_outpatient.well_visit_ind,
    stg_encounter_outpatient.sick_visit_ind,
    stg_encounter_outpatient.telehealth_ind,
    stg_encounter_outpatient.walkin_ind,
    stg_encounter_outpatient.online_scheduled_ind,
    stg_encounter_outpatient.mychop_scheduled_ind,
    stg_encounter_outpatient.international_ind,
    stg_encounter_outpatient.pat_key,
    referring_provider.provider_key as referring_provider_key,
    stg_encounter_outpatient.dept_key,
    stg_encounter_outpatient.prov_key,
    stg_encounter_outpatient.payor_key,
    stg_encounter_outpatient.patient_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from
    {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
    inner join {{source('cdw', 'visit')}} as visit
        on stg_encounter_outpatient.visit_key = visit.visit_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter_outpatient.encounter_key
    left join  {{source('cdw','referral_source')}} as referral_source
        on referral_source.ref_src_key = visit.ref_src_key
    left join {{source('cdw', 'provider')}} as provider
        on referral_source.rfl_prov_key = provider.prov_key
    left join {{ref('dim_provider')}} as referring_provider
        on provider.prov_id = referring_provider.prov_id
    left join {{ref('stg_patient_pcp_attribution')}} as stg_patient_pcp_attribution
        on stg_patient_pcp_attribution.pat_key = visit.pat_key
            and visit.eff_dt between stg_patient_pcp_attribution.start_date
                and stg_patient_pcp_attribution.end_date
where
    stg_encounter_outpatient.specialty_care_ind = '1'
