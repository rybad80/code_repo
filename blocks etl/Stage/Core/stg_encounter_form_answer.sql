
select
     {{dbt_utils.surrogate_key([
                'pat_enc_form_ans.pat_enc_csn_id',
                'pat_enc_form_ans.pat_id',
                '\'CLARITY\''
            ])
        }} as encounter_key,
pat_enc_form_ans.pat_enc_csn_id,
pat_enc_form_ans.line as encounter_seq_num,
pat_enc_form_ans.pat_id as patient_id,
cl_qanswer.answer_id as question_answer_id,
cl_qanswer.form_id,
cl_qform.form_name,
cl_qform.form_type_c as form_type_id,
cl_qanswer_qa.line as question_answer_seq_num,
dim_question.question_name, 
dim_question.display_name as form_question_text,
pat_enc_form_ans.qf_stt_chng_emp_id as stat_change_emp_id,
cl_qanswer_qa.quest_id,
cl_qanswer_qa.quest_answer::varchar(200) as quest_answer,
cl_qanswer_qa.quest_comment::varchar(200) as quest_comment,
cl_qanswer_qa.quest_line_num,
cl_qanswer_qa.varchar_answer::varchar(200) as varchar_answer,
cl_qanswer_qa.numeric_answer,
cl_qanswer_qa.float_answer,
zc_qf_status.title as survey_status,
cl_qanswer_qa.question_instant as answer_date,
pat_enc_form_ans.qf_stat_chng_inst as answer_change_date
from {{source('clarity_ods', 'cl_qanswer')}} as cl_qanswer
inner join {{source('clarity_ods', 'cl_qanswer_qa')}} as cl_qanswer_qa on
    cl_qanswer_qa.answer_id = cl_qanswer.answer_id
left join {{source('clarity_ods', 'cl_qform')}} as cl_qform on
    cl_qanswer.form_id = cl_qform.form_id
left join {{ref('dim_question')}} as dim_question on 
    cl_qanswer_qa.quest_id = dim_question.question_id
inner join {{source('clarity_ods', 'pat_enc_form_ans')}} as pat_enc_form_ans
    on  pat_enc_form_ans.qf_hqa_id = cl_qanswer.answer_id
left join {{source('clarity_ods', 'zc_qf_status')}} as zc_qf_status on
    pat_enc_form_ans.qf_status_c = zc_qf_status.qf_status_c
