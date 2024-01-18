{{ config(
	materialized='table',
	dist='visit_key',
	meta={
		'critical': true
	}
) }}

select
    stg_encounter_outpatient_raw.visit_key,
    stg_encounter_outpatient_raw.encounter_key,
    stg_encounter_outpatient_raw.patient_name,
    stg_encounter_outpatient_raw.mrn,
    stg_encounter_outpatient_raw.dob,
    stg_encounter_outpatient_raw.csn,
    stg_encounter_outpatient_raw.sex,
    stg_encounter_outpatient_raw.age_years,
    stg_encounter_outpatient_raw.age_months,
    stg_encounter_outpatient_raw.age_days,
    stg_encounter_outpatient_raw.encounter_date,
    stg_encounter_outpatient_raw.original_appointment_made_date,
    stg_encounter_outpatient_raw.appointment_made_date,
    stg_encounter_outpatient_raw.appointment_date,
    employee.full_nm as appointment_entry_employee,
    employee.emp_id as appointment_entry_employee_id,
    stg_encounter_outpatient_raw.appointment_cancel_date,
    stg_encounter_outpatient_raw.encounter_close_date,
    stg_encounter_outpatient_raw.scheduled_to_encounter_days,
    stg_encounter_outpatient_npv.last_completed_visit_key,
    stg_encounter_outpatient_npv.last_completed_encounter_date,
    stg_encounter_outpatient_npv.n_specialty,
    stg_encounter_outpatient_npv.new_patient_3yr_ind,
    stg_encounter_outpatient_npv.npv_appointment_lag_days,
    stg_encounter_outpatient_npv.physician_app_psych_visit_ind,
    stg_encounter_outpatient_raw.provider_key,
    stg_encounter_outpatient_npv.provider_id,
    stg_encounter_outpatient_npv.provider_name,
    stg_encounter_outpatient_npv.provider_type,
    stg_encounter_outpatient_raw.specialty_name,
    stg_encounter_outpatient_raw.department_key,
    stg_encounter_outpatient_raw.department_name,
    stg_encounter_outpatient_raw.department_center,
    stg_encounter_outpatient_raw.department_id,
    stg_encounter_outpatient_raw.appointment_status,
    stg_encounter_outpatient_raw.appointment_status_id,
    stg_encounter_outpatient_raw.patient_address_seq_num,
    stg_encounter_outpatient_raw.patient_address_zip_code,
    stg_encounter_outpatient_raw.visit_type,
    stg_encounter_outpatient_raw.visit_type_id,
    stg_encounter_outpatient_npv.payor_name,
    stg_encounter_outpatient_npv.payor_group,
    stg_encounter_outpatient_raw.encounter_type,
    stg_encounter_outpatient_raw.encounter_type_id,
    stg_encounter_outpatient_raw.patient_class,
    stg_encounter_outpatient_raw.patient_class_id,
    stg_encounter_outpatient_raw.intended_use_name,
    stg_encounter_outpatient_raw.intended_use_id,
    stg_encounter_outpatient_raw.revenue_location_group,
    stg_encounter_outpatient_raw.chop_market,
    stg_encounter_outpatient_raw.region_category,
    stg_encounter_outpatient_raw.start_visit_date,
    stg_encounter_outpatient_raw.check_in_date,
    stg_encounter_outpatient_raw.assign_room_date,
    stg_encounter_outpatient_raw.start_rooming_date,
    stg_encounter_outpatient_raw.done_rooming_date,
    stg_encounter_outpatient_raw.check_out_date,
    stg_encounter_outpatient_raw.complete_visit_date,
    stg_encounter_outpatient_raw.hospital_discharge_date,
    stg_encounter_outpatient_raw.scheduled_length_min,
    stg_encounter_outpatient_raw.actual_length_min,
    /* earliest of encounter date or appointment cancel date, for finding
    "next" completed appointment before original encounter date */
    stg_encounter_outpatient_raw.min_date,
    -- for next_encounter_all in scheduling_specialty_care_appointments
    stg_encounter_outpatient_raw.cancel_ind,
    stg_encounter_outpatient_raw.noshow_ind,
    stg_encounter_outpatient_raw.lws_ind, --left without seen
    stg_encounter_outpatient_raw.cancel_noshow_ind,
    stg_encounter_outpatient_raw.cancel_noshow_lws_ind,
    stg_encounter_outpatient_raw.past_appointment_ind,
    stg_encounter_outpatient_raw.cancel_24hr_ind,
    stg_encounter_outpatient_raw.cancel_48hr_ind,
    stg_encounter_outpatient_raw.international_ind,
    stg_encounter_outpatient_raw.primary_care_ind,
    stg_encounter_outpatient_raw.specialty_care_ind,
    stg_encounter_outpatient_raw.urgent_care_ind,
    case
        when npv_appointment_lag_days is not null then 1 else 0
    end as npv_lag_incl_ind,
    stg_encounter_outpatient_raw.first_newborn_encounter_ind,
    stg_diagnosis_medically_complex.complex_chronic_condition_ind,
    stg_diagnosis_medically_complex.medically_complex_ind,
    stg_diagnosis_medically_complex.tech_dependent_ind,
    stg_encounter_outpatient_raw.physician_service_level_ind,
    stg_encounter_outpatient_raw.appointment_ind,
    stg_encounter_outpatient_raw.office_visit_ind,
    stg_encounter_outpatient_raw.recurring_outpatient_ind,
    stg_encounter_outpatient_raw.well_visit_ind,
    stg_encounter_outpatient_raw.sick_visit_ind,
    case when stg_encounter_telehealth.visit_key is not null then 1 else 0 end as telehealth_ind,
    stg_encounter_outpatient_raw.telephone_visit_ind,
    case when telehealth_ind = 1 or telephone_visit_ind = 1 then 1 else 0 end as video_telephone_visit_ind,
    stg_encounter_outpatient_raw.walkin_ind,
    stg_encounter_outpatient_raw.online_scheduled_ind,
    stg_encounter_outpatient_npv.phys_app_psych_online_scheduled_ind,
    stg_encounter_outpatient_raw.encounter_closed_ind,
    stg_encounter_outpatient_raw.mychop_scheduled_ind,
    stg_encounter_outpatient_raw.scc_ind,
    stg_encounter_outpatient_raw.pat_key,
    stg_encounter_outpatient_raw.patient_key,
    stg_encounter_outpatient_raw.dept_key,
    stg_encounter_outpatient_npv.prov_key,
    stg_encounter_outpatient_npv.payor_key
from
    {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
    left join {{source('cdw', 'employee')}} as employee
        on employee.emp_key = stg_encounter_outpatient_raw.appt_entry_emp_key
    left join {{ref('stg_encounter_telehealth')}} as stg_encounter_telehealth
        on stg_encounter_telehealth.visit_key = stg_encounter_outpatient_raw.visit_key
    left join {{ref('stg_diagnosis_medically_complex')}} as stg_diagnosis_medically_complex
        on stg_diagnosis_medically_complex.visit_key = stg_encounter_outpatient_raw.visit_key
    inner join {{ref('stg_encounter_outpatient_npv')}} as stg_encounter_outpatient_npv
        on stg_encounter_outpatient_npv.encounter_key = stg_encounter_outpatient_raw.encounter_key
