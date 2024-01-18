/*
{
  "SUBJECT_AREA": "clarity",
  "WORKFLOW_NAME": "wf_clarity_data_lake",
  "WORKLET_NAME": "wklt_stg_miscellaneous",
  "SESSION_NAME": "s_stg_load_cl_visit_procedure_question",
  "MAPPING_NAME": "m_stg_load_cl_visit_procedure_question",
  "MAPPING_ID": 7923,
  "TARGET_ID": 7616,
  "TARGET_NAME": "s_cl_visit_procedure_question"
}
*/

with
sq_visit_procedure_question as (
    select
        qb.pat_enc_csn_id,
        a2.form_id,
        a1.line,
        q1.quest_id,
        null as question,
        a1.quest_answer,
        a1.question_instant,
        1 as src_id
    from
        {{ source('clarity_ods', 'ris_vst_bproc_ans') }} as qb
        inner join {{ source('clarity_ods', 'cl_qanswer_qa') }} as a1 on qb.begin_proc_ans_id = a1.answer_id
        inner join {{ source('clarity_ods', 'cl_qanswer') }} as a2 on a2.answer_id = a1.answer_id
        inner join
            {{ source('clarity_ods', 'cl_qquest_ovtm') }} as q1 on
                a1.quest_id = q1.quest_id and a1.quest_dat = q1.contact_date

    union -- noqa: L033

    select
        qb.pat_enc_csn_id,
        a2.form_id,
        a1.line,
        q1.quest_id,
        null as question,
        a1.quest_answer,
        a1.question_instant,
        2 as src_id
    from
        {{ source('clarity_ods', 'ris_vst_eproc_ans') }} as qb
        inner join {{ source('clarity_ods', 'cl_qanswer_qa') }} as a1 on qb.end_proc_ans_id = a1.answer_id
        inner join {{ source('clarity_ods', 'cl_qanswer') }} as a2 on a2.answer_id = a1.answer_id
        inner join
            {{ source('clarity_ods', 'cl_qquest_ovtm') }} as q1 on
                a1.quest_id = q1.quest_id and a1.quest_dat = q1.contact_date

    union -- noqa: L033

    select
        a.pat_enc_csn_id as csn,
        a2.form_id,
        a3.line,
        q2.quest_id,
        null as question,
        a3.quest_answer,
        a3.question_instant,
        3 as src_id
    from
        {{ source('clarity_ods', 'pat_enc_qnrs_ans') }} as a
        inner join {{ source('clarity_ods', 'cl_qanswer') }} as a2 on a.appt_qnrs_ans_id = a2.answer_id
        inner join {{ source('clarity_ods', 'cl_qanswer_qa') }} as a3 on a2.answer_id = a3.answer_id
        inner join
            {{ source('clarity_ods', 'cl_qquest_ovtm') }} as q2 on
                a3.quest_id = q2.quest_id and a3.quest_date_real = q2.contact_date_real
)
select
    cast(sq_visit_procedure_question.pat_enc_csn_id as bigint) as pat_enc_csn_id,
    cast(sq_visit_procedure_question.form_id as varchar(18)) as form_id,
    cast(sq_visit_procedure_question.line as bigint) as line,
    cast(sq_visit_procedure_question.quest_id as varchar(18)) as quest_id,
    cast(sq_visit_procedure_question.question as varchar(1000)) as question,
    cast(sq_visit_procedure_question.quest_answer as varchar(2000)) as quest_answer,
    cast(sq_visit_procedure_question.question_instant as timestamp) as question_instant,
    cast(sq_visit_procedure_question.src_id as numeric) as src_id
from sq_visit_procedure_question
