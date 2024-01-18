select
    redcap_detail.mstr_redcap_quest_key,
    master_redcap_question.field_order,
    master_redcap_question.field_nm,
    master_redcap_question.element_label,
    master_redcap_element_answr.element_id,
    master_redcap_element_answr.element_desc,
    /* Redcap Detail */
    redcap_detail.record as record_id,
    case when master_redcap_question.element_type in ('yesno', 'select') and value != '' then cast(value as int) end as value_int,
    case when master_redcap_question.element_type = 'calc' and value != '' then cast(value as float) end as value_float,
    substr(coalesce(element_desc, value), 1, 200) as value,
    row_number() over (partition by redcap_detail.record, redcap_detail.mstr_redcap_quest_key order by master_redcap_element_answr.element_id) as row_num,
    rsr.submission_timestamp
from
     {{ source('cdw', 'redcap_detail') }} as redcap_detail
     left join {{ source('cdw', 'master_redcap_project') }} as master_redcap_project on redcap_detail.mstr_project_key = master_redcap_project.mstr_project_key
     left join {{ source('cdw', 'master_redcap_question') }} as master_redcap_question on redcap_detail.mstr_redcap_quest_key = master_redcap_question.mstr_redcap_quest_key
     left join {{ source('cdw', 'master_redcap_element_answr') }} as master_redcap_element_answr on redcap_detail.mstr_redcap_quest_key = master_redcap_element_answr.mstr_redcap_quest_key and redcap_detail.value = master_redcap_element_answr.element_id
     left join ( --noqa: L042
           select
rsr.redcap_record,
                  rsr.mstr_redcap_event_key,
                  min(rsr.survey_response_first_submit_dt) as submission_timestamp
           from {{ source('cdw', 'master_redcap_survey_response') }} as rsr
           group by 1, 2
     ) as rsr on rsr.mstr_redcap_event_key = redcap_detail.mstr_redcap_event_key and rsr.redcap_record = redcap_detail.record
where
     redcap_detail.cur_rec_ind = 1 --current data
     and master_redcap_project.cur_rec_ind = 1
     and master_redcap_question.cur_rec_ind = 1
     and master_redcap_project.project_id = 95 --PIV Infiltration Outcomes - VAS Infiltration Grading Form
