with base as (
select
    visit_key,
    pat_key,
    question_answer_id as bh_screening_qa_id,
    question_answer_seq_num as bh_screening_seq_num,
    answer_date,
    form_id as bh_form_id,
    form_name as bh_screening_name,
    form_question_id as bh_screening_question_id,
    form_question_name as bh_screening_question_name,
    answer_as_numeric as bh_screening_answer,
    'bh_questions' as screening_type
from {{(ref('question_patient_answered'))}}
where form_id in
    ('100101',  -- WEL PHQ-9 MODIFIED FOR TEENS (DO NOT SEND TO MYCHOP)
        '35324529', -- PHQ-8 (PATIENT HEALTH QUESTIONNAIRE-8)
        '100103', -- WEL M-CHAT (MODIFIED CHECKLIST FOR AUTISM IN TODDLERS)
        '100139',  -- WEL M-CHAT-R 2016 (MODIFIED CHECKLIST FOR AUTISM IN TODDLERS) (MAIN)   
        '118602', -- VANDERBILT (NICHQ) PARENT F/U INATTENTIVE BRANCH NON-DOC FLOWSHEET (DON'T SEND)    
        '118603' -- VANDERBILT (NICHQ) PARENT F/U HYP/IMPULSIVE BRANCH NON-DOC FLOWSHEET (DON'T SEND)
        )
        and encounter_date >= '2018-01-01'

union all

select
    visit_key,
    pat_key,
    fs_rec_key as bh_screening_qa_id,
    seq_num as bh_screening_seq_num,
    recorded_date as answer_date,
    null as form_id,
    flowsheet_title as bh_screening_name,
    flowsheet_id as bh_screening_question_id,
    flowsheet_name as bh_screening_question_name,
    meas_val_num as bh_screening_answer,
   'nichq_flowsheets' as screening_type
from {{ref('flowsheet_all')}}
where flowsheet_id in
        (10060433,  -- Inattentive symptoms marked as 'often' and 'very often'
            10060434,  -- Hyperactive/Impulsive symptoms marked as 'often' and 'very often'
            10060435, -- Inattentive symptoms marked as 'often' and 'very often'
            10060436, -- Hyperactive/Impulsive symptoms marked as 'often' and 'very often'
            54760644, -- Inattentive score (6 or more plus 1 performance impairment is positive)
            54760645, -- Hyperactive score (6 or more plus 1 performance impairment is positive)
            54760708,  -- Inattentive score (6 or more plus 1 performance impairment is positive)
            54760709  -- Hyperactive score (6 or more plus 1 performance impairment is positive)
            )
    and encounter_date >= '2018-01-01'
)

select distinct
    visit_key,
    pat_key,
    bh_screening_qa_id,
    bh_screening_seq_num,
    answer_date,
    bh_screening_name,
    bh_screening_question_id,
    bh_screening_question_name,
    bh_screening_answer,
    screening_type
from base
