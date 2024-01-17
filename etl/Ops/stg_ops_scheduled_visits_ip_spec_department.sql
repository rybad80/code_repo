/*
Scheduled elective visits at the PHL and KOPH IP campuses based on the
visit department. Most admissions are based on the patient classes
"Admit before" and "Admit after" but there is an additional procedure
order, "Admit after procedure" which can be filled out for a patient
class of Outpatient.  An elective visit does not always result in an
admission and these visits are seperate from a visit for a surgery
in the Periop, Cardiac, and KOPH 3 units.
*/
with scheduled_visits as (
    select
        stg_encounter.pat_key,
        stg_encounter.visit_key,
        stg_encounter.encounter_date,
        visit_addl_info.der_hsp_svc as service_name,
        stg_department_all.department_name,
        cast(stg_encounter.patient_class_id as integer) as patient_class_id
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{ref('stg_department_all')}} as stg_department_all
            on stg_department_all.dept_key = stg_encounter.dept_key
        inner join {{source('cdw', 'visit_addl_info')}} as visit_addl_info
            on visit_addl_info.visit_key = stg_encounter.visit_key
        inner join {{ref('stg_patient')}} as stg_patient
            on stg_patient.pat_key = stg_encounter.pat_key
    where
        stg_encounter.encounter_date >= current_date
        and lower(stg_department_all.department_name) in (
            'main gi endoscopy ste', ' main sleep center',
            'main interventional', 'main mri', 'main ct scan', 'main ultrasound',
            'main fluoro radiology', 'main sedation unit', 'main cpru',
            'bed management cent*', '6 northeast', '6 west', 'elective inpt admit',
            '7 east picu', 'bgr mri', 'wood mri-mag resonance', 'wood pet radiology',
            'wood meg radiology', 'bgr day medicine', 'bgr pet mr',
            'bgr pet radiology', 'bgr sedation unit', 'koph patient placement',
            'koph scc ct scan', 'koph scc fluoro rad', 'koph scc mri',
            'koph scc nuclear med', 'koph scc sedation unit', 'koph scc ultrasound'
        )
        and lower(stg_patient.patient_name) not like '%unborn%' -- fetis gets visit along with sdu mom
),

visit_diagnosis as (
    select
        scheduled_visits.visit_key,
        diagnosis_encounter_all.diagnosis_name as diagnosis_name,
        diagnosis_encounter_all.icd10_code as icd10_code,
        row_number() over(
            partition by scheduled_visits.visit_key
            order by diagnosis_encounter_all.diagnosis_name
        ) as row_num
    from
        scheduled_visits
        left join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
            on diagnosis_encounter_all.visit_key = scheduled_visits.visit_key
    where
        (
            (
                diagnosis_encounter_all.marked_primary_ind = 1
                and diagnosis_encounter_all.ip_admit_primary_ind = 1
            )
            or (
                diagnosis_encounter_all.marked_primary_ind = 1
                and diagnosis_encounter_all.hsp_acct_admit_primary_ind = 1
            )
        )
        or diagnosis_encounter_all.visit_key is null
    group by
        scheduled_visits.visit_key,
        diagnosis_encounter_all.diagnosis_name,
        diagnosis_encounter_all.icd10_code
),

visit_procedure as (
    select
        scheduled_visits.visit_key,
        procedure_order_clinical.procedure_name as scheduled_procedure,
        row_number() over(
            partition by scheduled_visits.visit_key
            order by procedure_order_clinical.procedure_name
        ) as row_num
    from
        scheduled_visits
        left join {{ref('procedure_order_clinical')}} as procedure_order_clinical
            on procedure_order_clinical.visit_key = scheduled_visits.visit_key
    group by
        scheduled_visits.visit_key,
        procedure_order_clinical.procedure_name
),

admit_after_procedure as (
    select
        scheduled_visits.visit_key,
        case when visit_procedure_question.ansr = 'Yes' then 1 else 0 end as ansr_ind
    from
        scheduled_visits
        inner join {{source('cdw','visit_procedure_question')}} as visit_procedure_question
            on scheduled_visits.visit_key = visit_procedure_question.visit_key
        inner join {{source('cdw','master_question')}} as master_question
            on master_question.quest_key = visit_procedure_question.quest_key
    where
        master_question.quest_id = '189'
)

select
    visit_diagnosis.diagnosis_name,
    visit_diagnosis.icd10_code,
    'ELECTIVE ADMISSION' as visit_reason,
    scheduled_visits.encounter_date as scheduled_date,
    scheduled_visits.department_name as visit_department_name,
    scheduled_visits.service_name,
    visit_procedure.scheduled_procedure,
    case
        when lower(service_name) = 'oncology' then 'ONCO IP'
        when lower(service_name) = 'obstetrics' then 'SDU'
    end as scheduled_destination,
    null as patient_priority,
    null as expected_los_desc,
    1 as inpatient_ind,
    null as icu_ind,
    scheduled_visits.pat_key,
    scheduled_visits.visit_key,
    scheduled_visits.patient_class_id,
    coalesce(ansr_ind, 0) as admit_after_procedure_ind
from
    scheduled_visits
    left join visit_diagnosis
        on visit_diagnosis.visit_key = scheduled_visits.visit_key
        and visit_diagnosis.row_num = 1
    left join visit_procedure
        on visit_procedure.visit_key = scheduled_visits.visit_key
        and visit_procedure.row_num = 1
    left join admit_after_procedure
        on admit_after_procedure.visit_key = scheduled_visits.visit_key
