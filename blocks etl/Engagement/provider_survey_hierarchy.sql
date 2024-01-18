/* provider hierarchy */
select
    '2019' as survey_year,
    location_hierarchy,
    specialty_hierarchy,
    null as app_division_hierarchy,
    null as app_manager_hierarchy,
    null as cost_center_hierarchy,
    case
        when regexp_like(primary_hierarchy, '(\w*_\w*)')
        then regexp_replace(primary_hierarchy, '_', ' - ') -- replace any '_' with ' - '
        else primary_hierarchy end as division_cost_center_hierarchy, -- 2019 has backwards values (manager name - cost center) -- noqa: L016
    null as manager_hierarchy, -- generate manager hierarchy from primary hierarchy?
    prov_survey_hierarchy_2019.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'prov_survey_hierarchy_2019')}} as prov_survey_hierarchy_2019
    left join {{ref('lookup_provider_question_mapping')}} as lookup
        on prov_survey_hierarchy_2019.question = lookup.question_text_2019
union all
select
    '2020' as survey_year,
    null as location_hierarchy,
    null as specialty_hierarchy,
    app_division_hierarchy,
    app_manager_hierarchy,
    cost_center_hierarchy,
    division_cost_hierarchy as division_cost_center_hierarchy,
    manager_hierarchy,
    prov_survey_hierarchy_2020.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'prov_survey_hierarchy_2020')}} as prov_survey_hierarchy_2020
    left join {{ref('lookup_provider_question_mapping')}} as lookup
        on prov_survey_hierarchy_2020.question = lookup.question_text_2020
union all
select
    '2021' as survey_year,
    null as location_hierarchy,
    null as specialty_hierarchy,
    app_division_hierarchy,
    app_manager_hierarchy,
    cost_center_hierarchy,
    division_cost_center_hierarchy,
    manager_hierarchy,
    prov_survey_hierarchy_2021.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'prov_survey_hierarchy_2021')}} as prov_survey_hierarchy_2021
    left join {{ref('lookup_provider_question_mapping')}} as lookup
        on prov_survey_hierarchy_2021.question = lookup.question_text_2021
union all
select
    '2022' as survey_year,
    null as location_hierarchy,
    null as specialty_hierarchy,
    app_division_hierarchy,
    app_manager_hierarchy,
    cost_center_hierarchy, -- have duplicated data in parentheses
    division_cost_center_hierarchy,
    manager_hierarchy,
    prov_survey_hierarchy_2022.question as question_text,
    lookup.question,
    case when score != 'NA' then score end as score
from {{source('ods', 'prov_survey_hierarchy_2022')}} as prov_survey_hierarchy_2022
    left join {{ref('lookup_provider_question_mapping')}} as lookup
        on prov_survey_hierarchy_2022.question = lookup.question_text_2022
