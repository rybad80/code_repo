/* employee hierarchy */
select
    '2019' as survey_year,
    null as cost_center_hierarchy,
    manager_roll_up as manager_hierarchy,
    case
        when regexp_like(nurse_manager, '(\w* _\w*)')
        then regexp_replace(nurse_manager, ' _', '_') -- replace any ' _' separator with ' - '
        else nurse_manager end as nurse_hierarchy,
    unit as primary_hierarchy,
    emp_survey_hierarchy_2019.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'emp_survey_hierarchy_2019')}} as emp_survey_hierarchy_2019
    left join {{ref('lookup_employee_question_mapping')}} as lookup
        on emp_survey_hierarchy_2019.question = lookup.question_text_2019
union all
select
    '2020' as survey_year,
    cost_center_hierarchy,
    manager_hierarchy,
    nursing_hierarchy_use_if_nursing_hierarchy_is_different_than_primary_hierarchy as nurse_hierarchy,
    primary_hierarchy,
    emp_survey_hierarchy_2020.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'emp_survey_hierarchy_2020')}} as emp_survey_hierarchy_2020
    left join {{ref('lookup_employee_question_mapping')}} as lookup
        on emp_survey_hierarchy_2020.question = lookup.question_text_2020
union all
select
    '2021' as survey_year,
    cost_center_hierarchy,
    manager_hierarchy,
    nursing_hierarchy as nurse_hierarchy,
    primary_hierarchy,
    emp_survey_hierarchy_2021.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'emp_survey_hierarchy_2021')}} as emp_survey_hierarchy_2021
    left join {{ref('lookup_employee_question_mapping')}} as lookup
        on emp_survey_hierarchy_2021.question = lookup.question_text_2021
union all
select
    '2022' as survey_year,
    cost_center_hierarchy,
    manager_hierarchy,
    nurse_manager_hierarchy as nurse_hierarchy,
    unit_hierarchy as primary_hierarchy,
    emp_survey_hierarchy_2022_1.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'emp_survey_hierarchy_2022_1')}} as emp_survey_hierarchy_2022_1
    left join {{ref('lookup_employee_question_mapping')}} as lookup
        on emp_survey_hierarchy_2022_1.question = lookup.question_text_2022
union all
select
    '2022' as survey_year,
    cost_center_hierarchy,
    manager_hierarchy,
    nurse_manager_hierarchy as nurse_hierarchy,
    unit_hierarchy as primary_hierarchy,
    emp_survey_hierarchy_2022_2.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'emp_survey_hierarchy_2022_2')}} as emp_survey_hierarchy_2022_2
    left join {{ref('lookup_employee_question_mapping')}} as lookup
        on emp_survey_hierarchy_2022_2.question = lookup.question_text_2022
