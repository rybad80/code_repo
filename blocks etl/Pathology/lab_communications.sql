{{ config(materialized='table', dist='call_communication_id') }}

select
    stg_lab_communications.call_communication_id,
    stg_lab_communications.communication_id,
    stg_lab_communications.specimen_id,
    stg_lab_communications.test_id,
    stg_lab_communications.procedure_order_id,
    stg_lab_communications.lab_call_topic,
    stg_lab_communications.entry_date,
    stg_lab_communications.communication_instant_datetime,
    stg_lab_communications.update_instant_datetime,
    stg_lab_communications.max_communication_datetime,
    stg_lab_communications.contact_name,
    stg_lab_communications.communication_user_id,
    stg_lab_communications.communication_user_name,
    stg_lab_communications.phone_number,
    stg_note_text.note_text
from
    {{ref('stg_lab_communications')}} as stg_lab_communications
    left join {{ref('stg_note_text')}} as stg_note_text
        on stg_lab_communications.comm_cmt_note_id = stg_note_text.note_id
