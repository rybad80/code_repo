with bh_sde as (


select sde_key,
visit_key,
case when element_value = 'Gender' then 'Gender Clinic' else element_value end as element_value
from {{ref('smart_data_element_all')}}
where concept_id  = 'CHOPBH#653'
and context_name  = 'ENCOUNTER'
)


select
    stg_encounter_outpatient.visit_key,
    stg_encounter_outpatient.pat_key,
    stg_encounter_outpatient.patient_name,
    stg_encounter_outpatient.age_years,
    stg_encounter_outpatient.mrn,
    stg_encounter_outpatient.csn,
    stg_encounter_outpatient.encounter_date,
    stg_encounter_outpatient.encounter_close_date,
    stg_encounter_outpatient.visit_type,
    case when stg_encounter_outpatient.visit_type like '%EVAL%'
        or stg_encounter_outpatient.visit_type like '%NEW%'
        or stg_encounter_outpatient.visit_type like '%TESTING%'
        or stg_encounter_outpatient.visit_type like '%NPV%'
        or stg_encounter_outpatient.visit_type like '%ADOS%'
        or stg_encounter_outpatient.visit_type like '%NFU%'
        or stg_encounter_outpatient.visit_type like '%PHP%'
        or stg_encounter_outpatient.visit_type like '%CLD%'
        or stg_encounter_outpatient.visit_type like '%HMHK 60%'
        or stg_encounter_outpatient.visit_type like '%INTAKE%'
        then 1 end as npv_ind,
    case when npv_ind = 1 then 'Evaluation' else 'Follow-up' end as visit_type_category,
    case when visit_type_category = 'Follow-up' and dim_provider.provider_type != 'FELLOW' and (
            stg_encounter_outpatient.appointment_status_id in (6, 2))
        then date(stg_encounter_outpatient.appointment_date) + interval '7 days'
        when visit_type_category = 'Follow-up' and dim_provider.provider_type = 'FELLOW' and (
            stg_encounter_outpatient.appointment_status_id in (6, 2))
        then date(stg_encounter_outpatient.appointment_date) + interval '14 days'
        when visit_type_category = 'Evaluation' and dim_provider.provider_type != 'FELLOW' and (
            stg_encounter_outpatient.appointment_status_id in (6, 2))
        then date(stg_encounter_outpatient.appointment_date) + interval '14 days'
        when visit_type_category = 'Evaluation' and dim_provider.provider_type = 'FELLOW' and (
            stg_encounter_outpatient.appointment_status_id in (6, 2))
        then date(stg_encounter_outpatient.appointment_date) + interval '28 days'
    end as encounter_due_date,
    stg_encounter_outpatient.encounter_type,
    stg_encounter_outpatient.appointment_status,
    stg_encounter_outpatient.appointment_date,
    stg_encounter_outpatient.appointment_cancel_date,
    case when stg_encounter_outpatient.appointment_status_id in (-2, 2, 6) then 1 else 0 end as complete_ind,
    case when stg_encounter_outpatient.appointment_status_id  = 4 then 1 else 0 end as noshow_ind,
    case when stg_encounter_outpatient.appointment_status_id  = 5 then 1 else 0 end as lwbs_ind,
    case when stg_encounter_outpatient.appointment_status_id = 3 then 1 else 0 end as canceled_ind,
    case when stg_encounter_outpatient.appointment_status_id in (3, 4) then 1 else 0 end as cancel_noshow_ind,
    stg_encounter_outpatient.encounter_closed_ind,
    current_date - date(stg_encounter_outpatient.encounter_close_date) as days_since_closed,
    date(stg_encounter_outpatient.encounter_close_date)
        - stg_encounter_outpatient.encounter_date as days_encounter_open,
    case when stg_encounter_outpatient.encounter_closed_ind = 1 then 'Closed'
        when stg_encounter_outpatient.encounter_closed_ind = 0 then 'Open'
        end as encounter_open_status,
    case when (stg_encounter_outpatient.encounter_close_date > encounter_due_date)
            or (stg_encounter_outpatient.encounter_closed_ind = 0 and current_date > encounter_due_date)
        then 'Overdue'
        when (stg_encounter_outpatient.encounter_close_date <= encounter_due_date)
            or (stg_encounter_outpatient.encounter_closed_ind = 0 and current_date <= encounter_due_date)
        then 'On-time' end as encounter_overdue_status,
    case when encounter_overdue_status = 'Overdue' then 1 else 0 end as  overdue_ind,
    diagnosis_encounter_all.diagnosis_name as primary_diagnosis_name,
    diagnosis_encounter_all.icd10_code as primary_diagnosis_code,
    diagnosis_encounter_all.dx_key,
    stg_encounter_outpatient.provider_name,
    stg_encounter_outpatient.provider_id,
    stg_department_all.dept_key,
    stg_encounter_outpatient.department_name,
    stg_encounter_outpatient.department_id,
    stg_department_all.department_center_abbr,
    stg_department_all.department_center_id,
    case when bh_sde.element_value is not null then bh_sde.element_value
         when bh_dept.program is not null then bh_dept.program
    end as program_completed_encs,
    case when program_completed_encs = 'ABC' then 'Match'
        when program_completed_encs = 'ADHD' then 'Behavioral Regulation'
        when program_completed_encs = 'AIC' then 'Neurodevelopmental'
        when program_completed_encs = 'ARC' then 'General Outpatient'
        when program_completed_encs = 'CHAMP' then 'Match'
        when program_completed_encs = 'DBT' then 'Match'
        when program_completed_encs = 'Disruptive Behavior' then 'Behavioral Regulation'
        when program_completed_encs = 'Eating Disorder' then 'Eating Disorder'
        when program_completed_encs = 'FSIP' then 'Match'
        when program_completed_encs = 'Gender Clinic' then 'Match'
        when program_completed_encs = 'General Clinic' then 'General Outpatient'
        when program_completed_encs = 'Neurodevelopment' then 'Neurodevelopmental'
        when program_completed_encs = 'Psychosis' then 'Neurodevelopmental'
        when program_completed_encs = 'Trauma' then 'Match'
        when program_completed_encs = 'Young Child Clinic' then 'Neurodevelopmental'
    end as section,
    bh_dept.division,
    bh_dept.cost_cntr_nm,
	bh_dept.rev_loc_id,
    bh_dept.src_address,
    bh_dept.street_long_deg_x as loc_long,
    bh_dept.street_lat_deg_y as loc_lat,
    stg_encounter_outpatient.payor_group,
    stg_encounter_outpatient.patient_class,
    stg_encounter_outpatient.telehealth_ind,
    case when dim_provider.provider_type in
        ('FELLOW', 'Nurse Practitioner', 'Psychology Intern', 'Resident', 'Registered Nurse')
        then 'Non-attending'
        when dim_provider.provider_type in (
                'Licensed Professional Counselor',
                'Neuro-Psychologist',
                'Psychologist',
                'Physician',
                'Social Worker')
        then 'Attending'
	end as attending_nonattending,
    case
        when
            dim_provider.provider_type in
                ('FELLOW', 'Nurse Practitioner', 'Physician', 'Resident', 'Registered Nurse')
        then 'Medical'
        when dim_provider.provider_type in (
                'Licensed Professional Counselor',
                'Neuro-Psychologist',
                'Psychologist',
                'Psychology Intern',
                'Social Worker'
            )
		then 'Non-Medical'
	end as provider_group,
    dim_provider.provider_type as provider_type,
    stg_encounter_payor.payor_name,
    stg_patient.mailing_address_line1,
    stg_patient.mailing_address_line2,
    stg_patient.mailing_city,
    stg_patient.mailing_state,
    stg_patient.mailing_zip,
    stg_patient.county,
    stg_patient.race,
    stg_patient.ethnicity,
    stg_patient.email_address,
    stg_patient.deceased_ind,
    stg_mychop_status.mychop_activation_ind,
    stg_mychop_status.mychop_declined_ind,
    stg_patient_pcp_attribution.pcp_location as current_pcp_location,
    encounter_specialty_care.complex_chronic_condition_ind,
    encounter_specialty_care.medically_complex_ind,
    encounter_specialty_care.tech_dependent_ind,
    encounter_specialty_care.physician_service_level_ind,
    encounter_specialty_care.office_visit_ind,
    encounter_specialty_care.recurring_outpatient_ind,
    encounter_specialty_care.specialty_name,
    encounter_specialty_care.primary_care_location,
    encounter_specialty_care.appointment_made_date
    from
    {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
    inner join {{ref('stg_department_all')}} as stg_department_all
        on stg_department_all.dept_key = stg_encounter_outpatient.dept_key
    left join {{ref('bh_departments')}} as bh_dept
        on stg_department_all.dept_key = bh_dept.dept_key
    left join bh_sde as bh_sde
        on stg_encounter_outpatient.visit_key = bh_sde.visit_key
    inner join {{ref('dim_provider')}} as dim_provider
        on stg_encounter_outpatient.provider_id = dim_provider.prov_id
    inner join {{ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter_outpatient.visit_key = stg_encounter_payor.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_encounter_outpatient.pat_key = stg_patient.pat_key
    left join {{ref('encounter_specialty_care')}} as encounter_specialty_care
        on stg_encounter_outpatient.visit_key = encounter_specialty_care.visit_key
    left join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on stg_encounter_outpatient.visit_key = diagnosis_encounter_all.visit_key
        and diagnosis_encounter_all.visit_primary_ind  = 1
    left join {{ref('stg_mychop_status')}} as stg_mychop_status
        on stg_encounter_outpatient.pat_key = stg_mychop_status.pat_key
    left join {{ref('stg_patient_pcp_attribution')}} as stg_patient_pcp_attribution
        on stg_encounter_outpatient.pat_key = stg_patient_pcp_attribution.pat_key
        and stg_patient_pcp_attribution.end_date = '9999-12-31 00:00:00.000'
where
    stg_department_all.specialty_name = 'BEHAVIORAL HEALTH SERVICES'
    and stg_encounter_outpatient.encounter_date >= '2018-01-01'
    -- 'Office Visit' or 'Appointment'
    and stg_encounter_outpatient.encounter_type_id in (101, 50)
    -- 'COMPLETED' or 'ARRIVED' or 'NOT APPLICABLE'
    and stg_encounter_outpatient.appointment_status_id in (2, 6, -2, 4, 3, 5)
