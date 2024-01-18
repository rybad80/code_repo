{{ config(meta = {
    'critical': true
}) }}

/*Pull historical baseline data collected in REDCap*/
with rc as (
    select
        master_redcap_question.field_nm,
        redcap_detail.record,
        substr(coalesce(master_redcap_element_answr.element_desc, redcap_detail.value), 1, 250) as value
    from
        {{source('cdw', 'redcap_detail')}} as redcap_detail
        left join {{source('cdw', 'master_redcap_project')}} as master_redcap_project
            on master_redcap_project.mstr_project_key = redcap_detail.mstr_project_key
        left join {{source('cdw', 'master_redcap_question')}} as master_redcap_question
            on master_redcap_question.mstr_redcap_quest_key = redcap_detail.mstr_redcap_quest_key
        left join {{source('cdw', 'master_redcap_element_answr')}} as master_redcap_element_answr
            on master_redcap_element_answr.mstr_redcap_quest_key = redcap_detail.mstr_redcap_quest_key
            and redcap_detail.value = master_redcap_element_answr.element_id
    where
        redcap_detail.cur_rec_ind = 1
        and master_redcap_project.project_id = 119
        and master_redcap_question.field_nm  in ('medicalrecordnumber', 'contactdate', 'jia_type')
),

clean as (
    select
        rc.record as record_id,
        max(case when rc.field_nm = 'medicalrecordnumber' then rc.value end) as mrn,
        max(case when rc.field_nm = 'contactdate' then rc.value end) as contact_dt,
        max(case when rc.field_nm = 'jia_type' then rc.value end) as jia_type
    from
        rc
    group by
        rc.record
)

select
    clean.record_id,
    clean.mrn,
    clean.contact_dt,
    clean.jia_type,
    stg_patient.pat_key
from
    clean
    inner join {{ref('stg_patient')}} as stg_patient
    on clean.mrn = stg_patient.mrn
where
    jia_type is not null
