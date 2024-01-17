with stage as (
    select
        stg_encounter.visit_key,
        stg_encounter.encounter_date,
        stg_encounter.hospital_admit_date,
        stg_encounter.visit_type,
        stg_encounter.encounter_type,
        stg_encounter.patient_class,
        stg_encounter.age_years,
        stg_encounter.patient_address_zip_code,
        stg_department_all.department_name,
        stg_encounter_inpatient.admission_department_center_abbr,
        stg_encounter.admission_type,
        case
            when stg_encounter.hospital_admit_date is not null
            then coalesce(stg_encounter.hospital_discharge_date, current_date)
        end as hospital_disch_date_or_current_date,
        case
            when stg_encounter_outpatient_raw.primary_care_ind = 1
            then 'Primary Care'
            when lower(ops_completed_or_cases.location_name) = 'cardiac operative imaging complex'
            then 'Cardiac'
            when lower(ops_completed_or_cases.location_name) = 'periop complex'
            then 'Periop - Main'
            when lower(ops_completed_or_cases.location_name) = 'king of prussia hospital'
            then 'Periop - KOPH'
            when lower(ops_completed_or_cases.location_name) like '%day surgery%'
            then 'Periop - ASC'
            when radiology_depts.dept_id is not null
            then 'Radiology'
            when stg_encounter_outpatient_raw.specialty_care_ind = 1
            then 'Amb Specialty'
            when stg_encounter_outpatient_raw.urgent_care_ind = 1
            then 'Urgent Care'
            when lower(stg_encounter.admission_type) = 'elective'
            then 'IP Spec Medical - Elective'
            when lower(stg_encounter.admission_type) != 'elective'
            then 'IP Spec Medical - Emergent'
        end as subcommittee,
        case
            when stg_encounter_outpatient_raw.well_visit_ind = 1 then 'Well Visit'
            when stg_encounter_outpatient_raw.sick_visit_ind = 1 then 'Sick Visit'
            else 'Other Visit'
        end as primary_care_visit_type,
        stg_encounter.dept_key,
        stg_encounter.pat_key
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{ref('stg_department_all')}} as stg_department_all
            on stg_department_all.dept_key = stg_encounter.dept_key
        left join {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
            on stg_encounter_outpatient_raw.visit_key = stg_encounter.visit_key
        left join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
            on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
        left join {{ref('ops_completed_or_cases')}} as ops_completed_or_cases
            on ops_completed_or_cases.visit_key = stg_encounter.visit_key
            and ops_completed_or_cases.surg_num_per_visit = 1
        left join {{ref('lookup_ops_departments')}} as radiology_depts
            on radiology_depts.dept_id = stg_department_all.department_id
            and lower(radiology_depts.category) = 'radiology'
    where
        stg_encounter.encounter_date >= '2019-01-01'
        and stg_encounter.encounter_date < current_date
        and (
            ops_completed_or_cases.visit_key is not null
            or stg_encounter_outpatient_raw.primary_care_ind = 1
            or stg_encounter_outpatient_raw.specialty_care_ind = 1
            or stg_encounter_outpatient_raw.urgent_care_ind = 1
            or stg_encounter_inpatient.visit_key is not null
        )
)

select
    stage.visit_key,
    stg_patient.mrn,
    stg_patient.patient_name,
    stage.encounter_date,
    stage.hospital_admit_date,
    stage.hospital_disch_date_or_current_date,
    stage.age_years,
    stage.patient_address_zip_code,
    stage.department_name,
    stage.visit_type,
    stage.encounter_type,
    stage.patient_class,
    stage.admission_type as hospital_admit_type,
    stage.subcommittee as oversight_subcommittee_desc,
    case when dim_date.weekday_ind = 0 then 1 else 0 end as weekend_visit_ind,
    case when subcommittee = 'Primary Care' then 1 else 0 end as primary_care_ind,
    case when subcommittee = 'Periop - Main' then 1 else 0 end as periop_main_or_ind,
    case when subcommittee = 'Periop - KOPH' then 1 else 0 end as periop_koph_ind,
    case when subcommittee = 'Periop - ASC' then 1 else 0 end as periop_asc_ind,
    case when subcommittee = 'Cardiac' then 1 else 0 end as cardiac_case_ind,
    case when subcommittee = 'Radiology' then 1 else 0 end as radiology_ind,
    case when subcommittee = 'Amb Specialty' then 1 else 0 end as amb_specialty_ind,
    case when subcommittee = 'Urgent Care' then 1 else 0 end as urgent_care_ind,
    case when subcommittee = 'IP Spec Medical - Elective' then 1 else 0 end as inpatient_specialty_elective_ind,
    case when subcommittee = 'IP Spec Medical - Emergent' then 1 else 0 end as inpatient_specialty_emergent_ind,
    case
        when subcommittee like 'IP Spec Medical%'
        then stage.admission_department_center_abbr
    end as admission_department_center_abbr,
    stage.primary_care_visit_type,
    stage.dept_key,
    stage.pat_key
from
    stage
    inner join {{ref('dim_date')}} as dim_date
        on dim_date.full_date = stage.encounter_date
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = stage.pat_key
