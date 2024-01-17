--	patient has had at least one in-person encounter(hospital encounter/office visit/surgery)
-- at chop in the past 5 years
select
    stg_encounter.pat_key
from {{ref('stg_encounter')}} as stg_encounter
inner join {{ref('stg_patient')}} as stg_patient
    on stg_encounter.pat_key = stg_patient.pat_key
where
    stg_patient.current_age <= 30
    and stg_patient.deceased_ind = 0
    and year(add_months(stg_encounter.encounter_date, 6)) >= 2018
    and stg_encounter.encounter_type_id in (
                                                3,   --'hospital encounter'
                                                101, --'office visit'
                                                51   --'surgery'
                                                )
group by stg_encounter.pat_key
except
--if the patient only saw main cfdt/main cfdt ob gyn and did not see endo, they would be excluded
select
    stg_encounter.pat_key
from {{ref('stg_encounter')}} as stg_encounter
left join {{ ref('encounter_specialty_care') }} as encounter_specialty_care
    on stg_encounter.visit_key = encounter_specialty_care.visit_key
where
    year(add_months(stg_encounter.encounter_date, 6)) >= 2020
    --and stg_encounter.appointment_status_id = '2' --'completed'
group by stg_encounter.pat_key
having max(case when stg_encounter.department_id in (101013011, --'main cfdt'
                                                    101013012  --'main cfdt ob gyn'
                                                    ) then 1 else 0 end) = 1
    and max(case when stg_encounter.encounter_type_id = 101 --office visit
        and lower(encounter_specialty_care.specialty_name) = 'endocrinology' then 1 else 0 end) = 0
