--patient meets at least one of the five criteria below
--1.  patient had an office visit in a department specialty of endocrinology with either
--one of the following providers during the past 3 years
select
    stg_encounter.visit_key,
    stg_encounter.pat_key,
    stg_encounter.mrn,
    stg_encounter.encounter_date
from {{ ref('stg_frontier_thyroid_cohort_base_tmp') }} as cohort_base_tmp
inner join {{ref('stg_encounter')}} as stg_encounter
    on cohort_base_tmp.pat_key = stg_encounter.pat_key
inner join {{ ref('encounter_specialty_care') }} as encounter_specialty_care
    on stg_encounter.visit_key = encounter_specialty_care.visit_key
inner join {{source('cdw','provider')}} as provider
    on provider.prov_key = stg_encounter.prov_key
where
    provider.prov_id in ('10352', --'bauer, andrew j.'
                                '2006317' --'robbins, stephanie l'
                                )
    and year(add_months(stg_encounter.encounter_date, 6)) >= 2020
    and stg_encounter.encounter_type_id = 101 --'office visit'
    and lower(encounter_specialty_care.specialty_name) = 'endocrinology'
