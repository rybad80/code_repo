select
    pg_survey_questions.service,
    pg_survey_questions.varname as question_id,
    pg_survey_questions.question_text as question_name,
    pg_survey_questions.section as section_name,
    case when pg_survey_questions.standard = 'Y'
    then 1 else 0
    end as standard_question_ind,
    max(pg_survey_questions.upd_dt)
from
    {{source('ods', 'pg_survey_questions')}} as pg_survey_questions
where
    pg_survey_questions.section not in('About You', 'Background', 'Uncategorized Comments')
    and pg_survey_questions.varname not like 'CMPT%'
    and pg_survey_questions.varname not like 'IT%'
group by
    pg_survey_questions.service,
    pg_survey_questions.varname,
    pg_survey_questions.question_text,
    pg_survey_questions.section,
    standard_question_ind
