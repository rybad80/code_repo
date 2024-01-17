{{ config(meta = {
    'critical': false
}) }}
/* nursing_pfex_survey
unique row per survey_line for Qlik to resolve the ID
*/
select
    case survey_line_name
        when 'Specialty Care' then 's'
        when 'Primary Care' then 'p'
        else '' end
    || survey_line_id as nursing_pfex_survey_id,
    survey_line_id,
    survey_line_name
from
    {{ ref('stg_nursing_pfex_p3_dims') }}
group by
    survey_line_id,
    survey_line_name
