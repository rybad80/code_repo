select
    visit_key,
    patient_name,
    mrn,
    dob,
    csn,
    encounter_date,
    year(encounter_date) as calendar_year,
    provider_name,
    provider_id,
    visit_date,
    specialty_name,
    department_name,
    survey_line_name,
    section_name,
    question_name,
    question_id,
    comment_text,
    comment_ind,
    comment_valence,
    response_text,
    tbs_ind,
    mean_value,
    telehealth_survey_ind,
    case when response_text in (5) then 'Promoters'
        when response_text in (3, 4) then 'Passives'
        when response_text < 3 then 'Detractors'
        end as nps_calculation,
    pat_key,
    prov_key
from
     {{ ref('pfex_all') }}
where
    lower(survey_line_name) in ('specialty care', 'primary care')
    and encounter_date >= '2020-04-01' and encounter_date <= current_date
    and lower(question_id) in (
        'tmt1', -- ease of talking with the care provider over the video connection
        'tmt2', -- how well the video connection worked during your video visit
        'tmt3', -- how well the audio connection worked during your video visit
        'o4', -- likelihood of your recommending our practice to others
        'secttele' -- telemedicine technology section comments:
    )
