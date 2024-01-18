-- Flag whether patients have had lung transplants or CFRD in the past
select
    cf_pat_visits.pat_key,
    max(
        case
            when dx.visit_key is not null
                then 1
            when tp.pat_key is not null
                then 1
            else 0
        end
    ) as exclude_ind
from {{ref('stg_cf_base')}} as cf_pat_visits

left join {{ref('diagnosis_encounter_all')}} as dx
    on cf_pat_visits.pat_key = dx.pat_key
    and (
        -- patients who have diabetes on their problem list
        (lower(dx.icd10_code) like 'e84%' and lower(dx.diagnosis_name) like '%diabetes%'
            and dx.problem_list_ind = 1 and dx.problem_resolved_date is null
        )
        -- diagnoses of Diabetes Type 1
        or dx.icd10_code like 'E10%'
        -- diagnoses for pre/post lung transplants
        or dx.icd10_code like 'Z01.81%'
        )
-- pull patients who have had a lung transplant
left join {{ref('transplant_recipients')}} as tp
    on cf_pat_visits.pat_key = tp.pat_key
    and tp.organ_id = 2 -- lung transplants
    and tp.transplant_date is not null
group by cf_pat_visits.pat_key
