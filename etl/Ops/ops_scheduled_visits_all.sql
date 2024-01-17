with surg_sched as (
    select
        visit_key,
        visit_department_name,
        scheduled_destination,
        arc_destination,
        scheduled_procedure,
        diagnosis_name,
        service_name,
        icd10_code,
        inpatient_ind,
        icu_destination_ind,
        case
            when arc_destination != 'Home'
            then 1
            when arc_destination is null and scheduled_destination not in('Home', 'None', 'NOT APPLICABLE')
            then 1
            else 0
        end as scheduled_admission_ind
    from
        {{ref('ops_scheduled_or_cases')}}
    where
        lower(visit_department_name) in (
            'cardiac operative imaging complex',
            'periop complex',
            'king of prussia hospital'
        )
        or lower(visit_department_name) like '%day surgery%'
),

ip_sched as (
    select
        stg_encounter.visit_key,
        stg_encounter.pat_key,
        coalesce(
            stg_ops_scheduled_visits_ip_spec_department.diagnosis_name,
            stg_ops_scheduled_visits_ip_spec_procedure.diagnosis_name
        ) as diagnosis_name,
        coalesce(
            stg_ops_scheduled_visits_ip_spec_department.icd10_code,
            stg_ops_scheduled_visits_ip_spec_procedure.icd10_code
        ) as icd10_code,
        coalesce(
            stg_ops_scheduled_visits_ip_spec_department.visit_reason,
            stg_ops_scheduled_visits_ip_spec_procedure.visit_reason
        ) as visit_reason,
        coalesce(
            stg_ops_scheduled_visits_ip_spec_department.scheduled_date,
            stg_ops_scheduled_visits_ip_spec_procedure.scheduled_date
        ) as scheduled_date,
        coalesce(
            stg_ops_scheduled_visits_ip_spec_department.visit_department_name,
            stg_ops_scheduled_visits_ip_spec_procedure.visit_department_name
        ) as visit_department_name,
        coalesce(
            stg_ops_scheduled_visits_ip_spec_department.service_name,
            stg_ops_scheduled_visits_ip_spec_procedure.service_name
        ) as service_name,
        coalesce(
            stg_ops_scheduled_visits_ip_spec_department.scheduled_procedure,
            stg_ops_scheduled_visits_ip_spec_procedure.scheduled_procedure
        ) as scheduled_procedure,
        coalesce(
            stg_ops_scheduled_visits_ip_spec_department.scheduled_destination,
            stg_ops_scheduled_visits_ip_spec_procedure.scheduled_destination
        ) as scheduled_destination,
        coalesce(
            stg_ops_scheduled_visits_ip_spec_department.patient_priority,
            stg_ops_scheduled_visits_ip_spec_procedure.patient_priority
        ) as patient_priority,
        coalesce(
            stg_ops_scheduled_visits_ip_spec_department.expected_los_desc,
            stg_ops_scheduled_visits_ip_spec_procedure.expected_los_desc
        ) as expected_los_desc,
        case
            when stg_ops_scheduled_visits_ip_spec_department.patient_class_id = 2
            then 0
            else coalesce(
                    stg_ops_scheduled_visits_ip_spec_department.inpatient_ind,
                    stg_ops_scheduled_visits_ip_spec_procedure.inpatient_ind
                )
        end as inpatient_ind,
        stg_ops_scheduled_visits_ip_spec_procedure.icu_ind as icu_ind,
        case
            when stg_ops_scheduled_visits_ip_spec_department.admit_after_procedure_ind = 1
            then 1
            when stg_ops_scheduled_visits_ip_spec_procedure.visit_key is not null
            then 1
            else 0
        end as scheduled_admission_ind
    from
        {{ref('stg_encounter')}} as stg_encounter
        left join
            {{ref('stg_ops_scheduled_visits_ip_spec_department')}} as stg_ops_scheduled_visits_ip_spec_department
            on stg_ops_scheduled_visits_ip_spec_department.visit_key = stg_encounter.visit_key
        left join
            {{ref('stg_ops_scheduled_visits_ip_spec_procedure')}} as stg_ops_scheduled_visits_ip_spec_procedure
            on stg_ops_scheduled_visits_ip_spec_procedure.visit_key = stg_encounter.visit_key
    where
        stg_ops_scheduled_visits_ip_spec_department.visit_key is not null
        or stg_ops_scheduled_visits_ip_spec_procedure.visit_key  is not null
),

stage as (
    select
        stg_encounter.visit_key,
        stg_encounter.pat_key,
        stg_encounter.encounter_date,
        stg_encounter.age_years,
        stg_encounter.patient_address_zip_code,
        stg_department_all.department_name,
        stg_encounter.dept_key,
        stg_encounter.visit_type,
        stg_encounter.encounter_type,
        stg_encounter.patient_class,
        case
            when (surg_sched.visit_key is not null or ip_sched.visit_key is not null)
            then coalesce(surg_sched.scheduled_destination, ip_sched.scheduled_destination)
            when (surg_sched.visit_key is  null and ip_sched.visit_key is  null)
            then 'Home'
        end as scheduled_destination,
        surg_sched.arc_destination,
        case
            when stg_encounter_outpatient_raw.primary_care_ind = 1
            then 'Primary Care'
            when  lower(surg_sched.visit_department_name) = 'cardiac operative imaging complex'
            then 'Cardiac'
            when lower(surg_sched.visit_department_name) = 'periop complex'
            then 'Periop - Main OR'
            when lower(surg_sched.visit_department_name) = 'king of prussia hospital'
            then 'Periop - KOPH'
            when lower(surg_sched.visit_department_name) like '%day surgery%'
            then 'Periop - ASC'
            when  radiology_depts.dept_id is not null
            then 'Radiology'
            when stg_encounter_outpatient_raw.specialty_care_ind = 1 or ip_sched.inpatient_ind = 0
            then 'Amb Specialty'
            when ip_sched.inpatient_ind = 1
            then 'IP Spec Medical - Elective'
        end as subcommittee,
        coalesce(surg_sched.diagnosis_name, ip_sched.diagnosis_name) as diagnosis_name,
        coalesce(surg_sched.icd10_code, ip_sched.icd10_code) as icd10_code,
        coalesce(surg_sched.service_name, ip_sched.service_name) as service_name,
        coalesce(surg_sched.scheduled_procedure, ip_sched.scheduled_procedure) as scheduled_procedure,
        case
            when surg_sched.inpatient_ind = 1
                or ip_sched.inpatient_ind = 1
            then 1
            else 0
        end as inpatient_ind,
        case
            when surg_sched.icu_destination_ind = 1
                or ip_sched.icu_ind = 1
            then 1
            else 0
        end as icu_ind,
        coalesce(
            surg_sched.scheduled_admission_ind, ip_sched.scheduled_admission_ind, 0
        ) as scheduled_admission_ind
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{ref('stg_department_all')}} as stg_department_all
            on stg_department_all.dept_key = stg_encounter.dept_key
        left join {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
            on stg_encounter_outpatient_raw.visit_key = stg_encounter.visit_key
        left join surg_sched
            on surg_sched.visit_key = stg_encounter.visit_key
        left join ip_sched
            on ip_sched.visit_key = stg_encounter.visit_key
        left join {{ref('lookup_ops_departments')}} as radiology_depts
            on radiology_depts.dept_id = stg_department_all.department_id
            and lower(radiology_depts.category) = 'radiology'
    where
        stg_encounter.encounter_date >= current_date
        and stg_encounter.encounter_date < (this_week(current_date) + 21)
        and (
            surg_sched.visit_key is not null
            or ip_sched.visit_key is not null
            or stg_encounter_outpatient_raw.primary_care_ind = 1
            or stg_encounter_outpatient_raw.specialty_care_ind = 1
        )
)

select
    stage.visit_key,
    stage.pat_key,
    stg_patient.mrn,
    stg_patient.patient_name,
    stage.encounter_date,
    visit.appt_entry_dt as appointment_entry_date,
    stage.age_years,
    stage.patient_address_zip_code,
    stage.department_name,
    stage.dept_key,
    stage.visit_type,
    stage.encounter_type,
    stage.patient_class,
    stage.scheduled_destination,
    stage.arc_destination,
    stage.subcommittee as oversight_subcommittee_desc,
    case when stage.subcommittee = 'Primary Care' then 1 else 0 end as primary_care_ind,
    case when stage.subcommittee = 'Cardiac' then 1 else 0 end as cardiac_ind,
    case when stage.subcommittee = 'Periop - Main OR' then 1 else 0 end as periop_main_or_ind,
    case when stage.subcommittee = 'Periop - KOPH' then 1 else 0 end as periop_koph_ind,
    case when stage.subcommittee = 'Periop - ASC' then 1 else 0 end as periop_asc_ind,
    case when stage.subcommittee = 'Radiology' then 1 else 0 end as radiology_ind,
    case when stage.subcommittee = 'Amb Specialty' then 1 else 0 end as amb_specialty_ind,
    case
        when stage.subcommittee = 'IP Spec Medical - Elective' then 1 else 0
    end as inpatient_specialty_elective_ind,
    visit.hosp_admit_type as hospital_admit_type,
    stage.diagnosis_name,
    stage.icd10_code,
    stage.service_name,
    stage.scheduled_procedure,
    stage.inpatient_ind,
    stage.icu_ind,
    stage.scheduled_admission_ind
from
    stage
    inner join {{source('cdw', 'visit')}} as visit
        on visit.visit_key = stage.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = stage.pat_key
where
    subcommittee is not null
