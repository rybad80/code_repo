{{
    config(
        materialized='incremental',
        unique_key = 'smart_data_key'
    )
}}

select
    smart_data_element_value.sde_key,
    smart_data_element_value.seq_num,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    coalesce(stg_encounter.csn, note_stg_encounter.csn) as csn,
    coalesce(stg_encounter.encounter_date, note_stg_encounter.encounter_date) as encounter_date,
    clinical_concept.concept_id,
    clinical_concept.concept_desc as concept_description,
    smart_data_element_info.context_nm as context_name,
    case
        when lower(smart_data_element_info.context_nm) in ('encounter', 'patiententered', 'history')
            then 'visit.visit_key'
        when lower(smart_data_element_info.context_nm) in ('concept')
            then 'clinical_concept.concept_key'
        when lower(smart_data_element_info.context_nm) in ('result')
            then 'procedure_order_result_clinical.result_component_id'
        when lower(smart_data_element_info.context_nm) in ('patient')
            then 'patient.pat_id'
        when lower(smart_data_element_info.context_nm) in ('document')
            then 'document_info.doc_info_id'
        when lower(smart_data_element_info.context_nm) in ('note')
            then 'stg_note_info.note_id'
        when lower(smart_data_element_info.context_nm) in ('order')
            then 'procedure_order.proc_ord_id'
        when lower(smart_data_element_info.context_nm) in ( 'organ')
            then 'master_result_organism.organism_id'
        when lower(smart_data_element_info.context_nm) in ('problem')
            then 'patient_problem_list.prob_list_id'
        when lower(smart_data_element_info.context_nm) in ('registry')
            then 'registry_all_patient.record_id'
        when lower(smart_data_element_info.context_nm) in ('episode')
            then 'episode.epsd_id'
      end as linked_field,
    smart_data_element_info.rec_id_char,
    smart_data_element_info.rec_id_num as rec_id_numeric,
    smart_data_element_info.src_sys_val as epic_source_location,
    smart_data_element_value.elem_val as element_value,
    smart_data_element_value.elem_val_num as element_value_numeric,
    employee.emp_key as sde_entered_emp_key,
    employee.full_nm as sde_entered_employee,
    smart_data_element_info.entered_dt as entered_date,
    case
        when stg_note_info.note_key is null then 0 else stg_note_info.note_key
    end as note_key,
    clinical_concept.concept_key,
    smart_data_element_info.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, note_stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    coalesce(anesthesia_encounter_link.visit_key,
             stg_encounter.visit_key,
             stg_note_info.visit_key) as visit_key,
    anesthesia_encounter_link.anes_visit_key,
    {{
        dbt_utils.surrogate_key(
            ['smart_data_element_value.sde_key',
            'smart_data_element_value.seq_num'])
    }} as smart_data_key,
    smart_data_element_info.sde_id,
    case
        when smart_data_element_info.upd_dt >= coalesce(smart_data_element_value.upd_dt, '1900-01-01')
            then smart_data_element_info.upd_dt
        else smart_data_element_value.upd_dt
    end as block_last_update_date
from
    {{source('cdw', 'smart_data_element_info')}} as smart_data_element_info
    inner join {{source('cdw', 'smart_data_element_value')}} as smart_data_element_value
        on smart_data_element_info.sde_key = smart_data_element_value.sde_key
    inner join {{source('cdw', 'clinical_concept')}} as clinical_concept
        on smart_data_element_info.concept_key = clinical_concept.concept_key
    left join {{source('cdw', 'employee')}} as employee
        on employee.emp_key = smart_data_element_info.emp_key
    left join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = smart_data_element_info.visit_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = smart_data_element_info.pat_key
    left join {{source('cdw', 'anesthesia_encounter_link')}} as anesthesia_encounter_link
        on anesthesia_encounter_link.anes_visit_key = smart_data_element_info.visit_key
            and anesthesia_encounter_link.anes_visit_key != 0
    left join {{ref('stg_note_info')}} as stg_note_info
        -- need to exclude for non-note context recs in join
        on stg_note_info.note_id = case
            -- context_nm always uppercase
            when smart_data_element_info.context_nm = 'NOTE'
                then smart_data_element_info.rec_id_char
            else '0'
        end
    left join {{ref('stg_encounter')}} as note_stg_encounter
        on note_stg_encounter.csn = stg_note_info.pat_enc_csn_id
    left join {{ref('stg_hsp_acct_xref')}} as note_stg_hsp_acct_xref
        on note_stg_hsp_acct_xref.encounter_key = note_stg_encounter.encounter_key
where
    {{ limit_dates_for_dev(ref_date = 'coalesce(stg_encounter.encounter_date, note_stg_encounter.encounter_date)') }}
{% if is_incremental() %}
    and date(block_last_update_date) >= (select max(date(block_last_update_date) ) from {{ this }})
{% endif %}
