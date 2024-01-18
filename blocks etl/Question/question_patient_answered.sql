with all_answers as (
    select
        question_answer_id,
        answer_change_date,
        row_number() over(
            partition by question_answer_id
            order by answer_change_date desc
        ) as seq_num_desc
    from
       {{ref('stg_encounter_form_answer')}}
    group by
        question_answer_id,
        answer_change_date
)

select
    stg_encounter.encounter_key,
    stg_encounter.visit_key,
    stg_encounter_form_answer.encounter_seq_num,
    stg_encounter_form_answer.question_answer_id,
    stg_encounter_form_answer.question_answer_seq_num,
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.csn,
    stg_encounter.sex,
    stg_patient.race,
    stg_patient.ethnicity,
    stg_encounter.dob,
    stg_encounter.age_years,
    stg_encounter.encounter_date,
    stg_encounter.department_name,
    stg_encounter.encounter_type,
    stg_encounter_form_answer.answer_change_date as answer_date,
    stg_encounter_form_answer.form_name,
    stg_encounter_form_answer.form_id,
    stg_encounter_form_answer.form_type_id,
    stg_encounter_form_answer.quest_id as form_question_id,
    stg_encounter_form_answer.question_name as form_question_name,
    stg_encounter_form_answer.form_question_text as form_question_text,
    stg_encounter_form_answer.quest_answer as answer_as_string,
    coalesce(stg_encounter_form_answer.numeric_answer,
            cast(stg_encounter_form_answer.float_answer as numeric)
            ) as answer_as_numeric,
    stg_encounter_form_answer.survey_status,
    case when lower(survey_status) like 'completed%' then 1 else 0 end as survey_complete_ind,
    stg_encounter.pat_key
from
    {{ref('stg_encounter_form_answer')}} as stg_encounter_form_answer
    inner join all_answers on all_answers.question_answer_id = stg_encounter_form_answer.question_answer_id
    inner join
        {{ref('stg_encounter')}} as stg_encounter on
            stg_encounter.encounter_key = stg_encounter_form_answer.encounter_key
    inner join {{ref('stg_patient')}} as stg_patient on stg_patient.pat_key = stg_encounter.pat_key
where
    --the same questionnaire can have multiple versions if not completed in the first instance
    --it seems they are only be differentiated by date
    all_answers.seq_num_desc = 1 --this allows us to get the right instance of the date
    and stg_encounter_form_answer.answer_change_date = all_answers.answer_change_date
