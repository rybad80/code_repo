select
    stg_ipc_pat_prov_med_union.visit_key,
    stg_ipc_pat_prov_med_union.action_key,
    stg_ipc_pat_prov_med_union.action_seq_num,
    stg_ipc_pat_prov_med_union.emp_key,
    stg_ipc_pat_prov_med_union.prov_key,
    stg_ipc_pat_prov_med_union.employee_name,
    stg_ipc_pat_prov_med_union.event_date,
    stg_ipc_pat_prov_med_union.event_description,
    stg_ipc_pat_prov_med_union.event_location,
    stg_ipc_pat_prov_med_union.action_key_field,
    'Medication' as event_type
from
    {{ref('stg_ipc_pat_prov_med_union')}} as stg_ipc_pat_prov_med_union
union all
select
    stg_ipc_pat_prov_flowsheet_union.visit_key,
    stg_ipc_pat_prov_flowsheet_union.action_key,
    stg_ipc_pat_prov_flowsheet_union.action_seq_num,
    stg_ipc_pat_prov_flowsheet_union.emp_key,
    stg_ipc_pat_prov_flowsheet_union.prov_key,
    stg_ipc_pat_prov_flowsheet_union.employee_name,
    stg_ipc_pat_prov_flowsheet_union.event_date,
    stg_ipc_pat_prov_flowsheet_union.event_description,
    stg_ipc_pat_prov_flowsheet_union.event_location,
    stg_ipc_pat_prov_flowsheet_union.action_key_field,
    'Flowsheet' as event_type
from
    {{ref('stg_ipc_pat_prov_flowsheet_union')}} as stg_ipc_pat_prov_flowsheet_union
union all
select
    stg_ipc_pat_prov_treatment_team.visit_key,
    stg_ipc_pat_prov_treatment_team.action_key,
    stg_ipc_pat_prov_treatment_team.action_seq_num,
    stg_ipc_pat_prov_treatment_team.emp_key,
    stg_ipc_pat_prov_treatment_team.prov_key,
    stg_ipc_pat_prov_treatment_team.employee_name,
    stg_ipc_pat_prov_treatment_team.event_date,
    stg_ipc_pat_prov_treatment_team.event_description,
    stg_ipc_pat_prov_treatment_team.event_location,
    stg_ipc_pat_prov_treatment_team.action_key_field,
    'Treatment team' as event_type
from
    {{ref('stg_ipc_pat_prov_treatment_team')}} as stg_ipc_pat_prov_treatment_team
union all
select
    stg_ipc_pat_prov_note_event_edit.visit_key,
    stg_ipc_pat_prov_note_event_edit.action_key,
    stg_ipc_pat_prov_note_event_edit.action_seq_num,
    stg_ipc_pat_prov_note_event_edit.emp_key,
    stg_ipc_pat_prov_note_event_edit.prov_key,
    stg_ipc_pat_prov_note_event_edit.employee_name,
    stg_ipc_pat_prov_note_event_edit.event_date,
    stg_ipc_pat_prov_note_event_edit.event_description,
    stg_ipc_pat_prov_note_event_edit.event_location,
    stg_ipc_pat_prov_note_event_edit.action_key_field,
    'Note event edit' as event_type
from
    {{ref('stg_ipc_pat_prov_note_event_edit')}} as stg_ipc_pat_prov_note_event_edit
union all
select
    stg_ipc_pat_prov_proc_union.visit_key,
    stg_ipc_pat_prov_proc_union.action_key,
    stg_ipc_pat_prov_proc_union.action_seq_num,
    stg_ipc_pat_prov_proc_union.emp_key,
    stg_ipc_pat_prov_proc_union.prov_key,
    stg_ipc_pat_prov_proc_union.employee_name,
    stg_ipc_pat_prov_proc_union.event_date,
    stg_ipc_pat_prov_proc_union.event_description,
    stg_ipc_pat_prov_proc_union.event_location,
    stg_ipc_pat_prov_proc_union.action_key_field,
    'Procedure' as event_type
from
    {{ref('stg_ipc_pat_prov_proc_union')}}  as stg_ipc_pat_prov_proc_union
union all
select
    stg_ipc_pat_prov_ambulatory_event.visit_key,
    stg_ipc_pat_prov_ambulatory_event.action_key,
    stg_ipc_pat_prov_ambulatory_event.action_seq_num,
    stg_ipc_pat_prov_ambulatory_event.emp_key,
    stg_ipc_pat_prov_ambulatory_event.prov_key,
    stg_ipc_pat_prov_ambulatory_event.employee_name,
    stg_ipc_pat_prov_ambulatory_event.event_date,
    stg_ipc_pat_prov_ambulatory_event.event_description,
    stg_ipc_pat_prov_ambulatory_event.event_location,
    stg_ipc_pat_prov_ambulatory_event.action_key_field,
    'Ambulatory' as event_type
from
    {{ref('stg_ipc_pat_prov_ambulatory_event')}} as stg_ipc_pat_prov_ambulatory_event
union all
select
    stg_ipc_pat_prov_ed_event.visit_key,
    stg_ipc_pat_prov_ed_event.action_key,
    stg_ipc_pat_prov_ed_event.action_seq_num,
    stg_ipc_pat_prov_ed_event.emp_key,
    stg_ipc_pat_prov_ed_event.prov_key,
    stg_ipc_pat_prov_ed_event.employee_name,
    stg_ipc_pat_prov_ed_event.event_date,
    stg_ipc_pat_prov_ed_event.event_description,
    stg_ipc_pat_prov_ed_event.event_location,
    stg_ipc_pat_prov_ed_event.action_key_field,
    'ED event' as event_type
from
    {{ref('stg_ipc_pat_prov_ed_event')}} as stg_ipc_pat_prov_ed_event
union all
select
    stg_ipc_pat_prov_or.visit_key,
    stg_ipc_pat_prov_or.action_key,
    stg_ipc_pat_prov_or.action_seq_num,
    stg_ipc_pat_prov_or.emp_key,
    stg_ipc_pat_prov_or.prov_key,
    stg_ipc_pat_prov_or.employee_name,
    stg_ipc_pat_prov_or.event_date,
    stg_ipc_pat_prov_or.event_description,
    stg_ipc_pat_prov_or.event_location,
    stg_ipc_pat_prov_or.action_key_field,
    'OR' as event_type
from
    {{ref('stg_ipc_pat_prov_or')}} as stg_ipc_pat_prov_or
