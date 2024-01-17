/* nursing_redcap_detail
has a row for each field/element of data for each redcap project record
for the porter redcap projects that nursing uses
*/
with project_rec_info as (
    select
        rsr.redcap_record,
        rsr.mstr_redcap_event_key,
        upper(max(rsr.survey_response_return_cd)) as return_cd,
        min(rsr.survey_response_first_submit_dt) as submit_dt
    from
        {{ source('cdw', 'master_redcap_survey_response') }} as rsr
    group by
        rsr.redcap_record,
        rsr.mstr_redcap_event_key
),


redcap_field_data as (
    select
        rcp.app_title as redcap_project_title,
        rcp.project_id as redcap_project_id,
        rcq.mstr_redcap_quest_key,
        rcq.field_order,
        rcq.field_nm as field_name,
        rcq.element_label,
        rcd.record as record_id,
        rcea.element_id,
        rcd.value as record_value,
        substr(
            coalesce(rcea.element_desc, rcd.value),
            1, 500) as field_value,
        row_number()
            over (partition by rcp.project_id, rcd.record
            order by  rcd.mstr_redcap_quest_key, rcd.value, rcea.element_id) as field_row_number,
        project_rec_info.submit_dt as record_submit_date
    from
        {{ source('cdw', 'redcap_detail') }} as rcd
        inner join {{ source('cdw', 'master_redcap_project') }} as rcp
            on rcp.mstr_project_key = rcd.mstr_project_key
        inner join {{ ref('lookup_nursing_redcap_project') }} as lookup_nursing_redcap_project
            on rcp.project_id = lookup_nursing_redcap_project.project_id
        left join {{ source('cdw', 'master_redcap_question') }} as rcq
            on rcq.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key
        left join {{ source('cdw', 'master_redcap_element_answr') }} as rcea
            on rcea.mstr_redcap_quest_key = rcd.mstr_redcap_quest_key
            and rcd.value = rcea.element_id
        left join project_rec_info
            on project_rec_info.mstr_redcap_event_key = rcd.mstr_redcap_event_key
            and project_rec_info.redcap_record = rcd.record
    where
        rcd.cur_rec_ind = 1
)

select
    redcap_project_id,
    record_id,
    field_order,
    field_name,
    record_value,
    field_value,
    element_label,
    mstr_redcap_quest_key,
    element_id,
    field_row_number,
    record_submit_date,
    redcap_project_title
from
    redcap_field_data
where mstr_redcap_quest_key is not null
