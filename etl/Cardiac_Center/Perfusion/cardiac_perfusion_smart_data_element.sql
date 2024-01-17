select
    sde_key,
    seq_num,
    cardiac_perfusion_surgery.patient_name,
    cardiac_perfusion_surgery.mrn,
    cardiac_perfusion_surgery.dob,
    cardiac_perfusion_surgery.csn,
    encounter_date,
    concept_id,
    concept_description,
    context_name,
    linked_field,
    rec_id_char,
    rec_id_numeric,
    epic_source_location,
    element_value,
    element_value_numeric,
    sde_entered_emp_key,
    sde_entered_employee,
    entered_date,
    note_key,
    concept_key,
    cardiac_perfusion_surgery.pat_key,
    hsp_acct_key,
    cardiac_perfusion_surgery.visit_key,
    cardiac_perfusion_surgery.anes_visit_key,
    smart_data_key,
    block_last_update_date,
    cardiac_perfusion_surgery.log_key
from
     {{ref('cardiac_perfusion_surgery')}} as cardiac_perfusion_surgery
     inner join {{ref('smart_data_element_all')}} as smart_data_element_all
     on cardiac_perfusion_surgery.anes_event_visit_key = smart_data_element_all.visit_key
