{{ config(meta = {
    'critical': false
}) }}
/* stg_nursing_pfex_p1_non_koph
capture just the nursing sections questions' top box score booleans,
values, & distinct_count_field for metrics in
the Nursing Dashboard Patient & Family Experience sheet
as aligned to department
*/

select
    survey_key,
    visit_date as metric_date,
    response_text,
    mean_value,
    tbs_ind as numerator,
    1 as denominator,
    survey_line_id,
    survey_line_name,
    section_name,
    question_id,
    question_name,
    initcap(specialty_name) as specialty_name,
    dept_key, /* department patient exited the visit (discharge if IP) */
    visit_key as distinct_count_field /* to use for survey counts */
from
    {{ ref('pfex_all') }}
where
    visit_date >= '2019-12-15'
    and (
    (lower(survey_line_name) in (
        'inpatient pediatric',
        'nicu',
        'pediatric ed')
        and section_name = 'Nurses')
    or (lower(survey_line_name) in (
        'specialty care',
        'primary care') /*  not right now: 'adult specialty care', 'urgent care' */
        and section_name in (
            'Nurses',
            'Nurse/assistant'))
    or (lower(survey_line_name) in (
        'inpatient pediatric',
        'nicu',
        'pediatric ed',
        'specialty care',
        'primary care')
        and question_id in (
            /* additional other section questions around:
            Cleanliness of our practice
            How well staff met your emotional needs
            Response to concerns/complaints made during your visit &
            How well the staff worked together */
            'N31',
            'F24',
            'I104',
            'I26',
            'I4',
            'O15',
            'O2',
            'OA2'))
/* not right now:
    or (lower(survey_line_name) = 'day surgery'  -- excluding 'Day Surgery - CPRU','SDU' survey lines
        and department_name not in ('6 NORTHEAST','PERIOP COMPLEX'))
   */
   )
