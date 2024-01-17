with preop_dx as (
    select
        or_case.or_case_id,
        diagnosis.dx_nm as diagnosis_name,
        diagnosis.icd10_cd as icd10_code
    from
        {{ref('procedure_order_clinical')}} as procedure_order_clinical
        inner join {{source('cdw', 'procedure_order')}} as proc_order
            on proc_order.proc_ord_key = procedure_order_clinical.proc_ord_key
        inner join {{source('cdw', 'procedure_order_diagnosis')}} as procedure_order_diagnosis
            on procedure_order_diagnosis.proc_ord_key = proc_order.proc_ord_key
        inner join {{source('cdw', 'diagnosis')}} as diagnosis
            on diagnosis.dx_key = procedure_order_diagnosis.dx_key
        inner join {{source('cdw', 'or_case_order')}} as or_case_order
            on or_case_order.ord_key = proc_order.proc_ord_key
        inner join {{source('cdw', 'or_case')}} as or_case
            on or_case.or_case_key = or_case_order.or_case_key
    where
        lower(procedure_name) like '%surgical case request%'
        and icd10_ind = 1
        and (diagnosis.seq_num = 1 or diagnosis.seq_num is null)
        and (procedure_order_diagnosis.seq_num = 1 or procedure_order_diagnosis.seq_num is null)
),

surg_pred as (
    select
        or_case.or_case_id,
        m_quest.latest_display_quest_nm,
        m_quest.quest_nm,
        case
            when ansr = 100 then '< 23 Hours'
            when ansr = 105 then '23 - 48 Hours'
            when ansr = 110 then '48 - 72 Hours'
            when ansr = 115 then '> 72 Hours'
            else null
        end as questionnaire_los
    from
        {{source('cdw', 'order_question')}} as order_question
        inner join {{source('cdw', 'master_question')}} as m_quest
            on order_question.quest_key = m_quest.quest_key
        inner join {{source('cdw', 'order_xref')}} as order_xref
            on order_xref.ord_key = order_question.ord_key
        inner join {{source('cdw', 'or_case_order')}} as or_case_order
            on or_case_order.ord_key = order_question.ord_key
        inner join {{source('cdw', 'or_case')}} as or_case
            on or_case_order.or_case_key = or_case.or_case_key
    where
        m_quest.quest_id = '900100174' --CHOP OPT EXPECTED LENGTH OF STAY
),

arc_destination as (
	select
        or_case.or_case_key,
        group_concat(
        case
            when clinical_concept.concept_id = 'CHOPANES#008'
            then  smart_data_element_value.elem_val
            end,
        ';'
        ) as final_pat_dest
    from
        {{source('cdw','or_case')}} as or_case
    inner join {{source('cdw','anesthesia_encounter_link')}} as anesthesia_encounter_link
        on anesthesia_encounter_link.or_case_key = or_case.or_case_key
	inner join {{source('cdw','smart_data_element_info')}} as smart_data_element_info
        on smart_data_element_info.visit_key = anesthesia_encounter_link.anes_event_visit_key
	inner join {{source('cdw','smart_data_element_value')}} as smart_data_element_value
        on smart_data_element_info.sde_key = smart_data_element_value.sde_key
    inner join {{source('cdw','clinical_concept')}} as clinical_concept
        on clinical_concept.concept_key = smart_data_element_info.concept_key
	where
        lower(smart_data_element_info.src_sys_val) = 'smartform 283' -- unique ID of the anes pre plan smart form
        and lower(clinical_concept.concept_id) = 'chopanes#008'  --CHOP ANES FINAL PATIENT DESTINATION
        and smart_data_element_value.elem_val is not null
	group by
        or_case.or_case_key
)

select
    surgery_encounter.case_key as or_case_key,
    stg_patient.patient_name,
    {{
        dbt_chop_utils.datetime_diff(
            from_date='stg_patient.dob',
            to_date='surgery_encounter.surgery_date',
            unit='hour'
        )
    }} as patient_age_years,
    stg_encounter.patient_address_zip_code,
    preop_dx.diagnosis_name,
    preop_dx.icd10_code,
    'SURGERY' as visit_reason,
    surgery_encounter.surgery_date as scheduled_date,
    surgery_encounter.location as visit_department_name,
    surgery_encounter.service as service_name,
    surgery_encounter.first_panel_first_procedure_name as scheduled_procedure,
    dict_loc.dict_nm as scheduled_destination,
    arc_destination.final_pat_dest as arc_destination,
    dict_priority.dict_nm as patient_surgical_priority,
    surg_pred.questionnaire_los as expected_los_desc,
    case
        when visit.hosp_admit_dt < surgery_encounter.surgery_date then 1
        else 0
    end as inpatient_ind,
    case when lower(dict_loc.dict_nm) like '%icu%' then 1 else 0 end as icu_destination_ind,
    stg_patient.mrn,
    stg_encounter.pat_key,
    or_case.admit_visit_key as visit_key
from
    {{ref('surgery_encounter')}} as surgery_encounter
    inner join {{source('cdw', 'or_case')}} as or_case
        on or_case.or_case_key = surgery_encounter.case_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_loc
        on dict_loc.dict_key = or_case.dict_or_post_dest_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_priority
        on dict_priority.dict_key = or_case.dict_or_prty_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = or_case.admit_visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_encounter.pat_key = stg_patient.pat_key
    inner join {{source('cdw', 'visit')}} as visit
        on visit.visit_key = or_case.admit_visit_key
    left join preop_dx
        on preop_dx.or_case_id = or_case.or_case_id
    left join surg_pred
        on surg_pred.or_case_id = or_case.or_case_id
    left join arc_destination
        on arc_destination.or_case_key = or_case.or_case_key
where
    surgery_encounter.surgery_date >= current_date
