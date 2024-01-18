{{ config(meta = {
    'critical': false
}) }}
/* nursing_pfex_question
unique row per question for Qlik to resolve the ID
because Question IDs represent different questions in
different survey lines, the survey_line_id needs to be included
for the PK
*/
select
    survey.nursing_pfex_survey_id,
    survey.nursing_pfex_survey_id || '^' || dims.pfex_question_id as nursing_pfex_question_id,
    dims.question_name,
    dims.section_name,
    case dims.section_name
        when 'Nurses' then 1
        when 'Nurse/assistant' then 1
        else 0
    end as entire_section_ind,
    dims.survey_line_id,
    dims.pfex_question_id
from
    {{ ref('stg_nursing_pfex_p3_dims') }} as dims
    inner join {{ ref('nursing_pfex_survey') }} as survey
        on dims.survey_line_id = survey.survey_line_id
        and dims.survey_line_name = survey.survey_line_name
group by
    survey.nursing_pfex_survey_id,
    dims.question_name,
    dims.section_name,
    dims.survey_line_id,
    dims.pfex_question_id
