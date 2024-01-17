{{ config(meta = {
    'critical': true
}) }}

select distinct
    stg_encounter.visit_key,
    1 as diabetes_ind
from
    {{ref('stg_encounter')}} as stg_encounter
    inner join {{ref('stg_department_all')}} as stg_department_all
        on stg_encounter.dept_key = stg_department_all.dept_key
    inner join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on diagnosis_encounter_all.visit_key = stg_encounter.visit_key
    inner join {{source('cdw', 'epic_grouper_diagnosis')}} as epic_grouper_diagnosis
        on epic_grouper_diagnosis.dx_key = diagnosis_encounter_all.dx_key
    inner join {{source('cdw', 'epic_grouper_item')}} as epic_grouper_item
        on epic_grouper_item.epic_grouper_key = epic_grouper_diagnosis.epic_grouper_key
where
    lower(epic_grouper_item.epic_grouper_nm) = 'chop icd diabetes registry'
    and lower(stg_department_all.specialty_name) = 'endocrinology'
