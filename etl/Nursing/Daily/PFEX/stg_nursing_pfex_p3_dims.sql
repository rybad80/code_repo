{{ config(meta = {
    'critical': false
}) }}
/* stg_nursing_pfex_p3_dims
collect the survey_line and question dimension data
*/
select
    survey_line_id,
    survey_line_name,
    section_name,
    question_id as pfex_question_id,
    question_name
from
    {{ ref('stg_nursing_pfex_p1_non_koph') }}
group by
    survey_line_id,
    survey_line_name,
    section_name,
    question_id,
    question_name
