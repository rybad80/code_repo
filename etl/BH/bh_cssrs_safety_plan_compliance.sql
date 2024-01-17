with
safety_plan_visit as (
select
    smart_data_element_all.visit_key,
    max(case when smart_data_element_all.concept_id = 'CHOPBH#663'
            and cast(
                trim(smart_data_element_all.element_value) as int
            ) = 1 then smart_data_element_all.entered_date
    end) as bh_safety_plan_sde,
    max(case when smart_data_element_all.concept_id = 'EPIC#31000169363'
        then smart_data_element_all.entered_date
    end) as stanley_brown_update_visit,
    max(case when smart_data_element_all.concept_id = 'EPIC#31000169338'
        then smart_data_element_all.entered_date
    end) as stanley_brown_creation_visit,
    max(case when smart_data_element_all.concept_id = 'CHOPBH#664'
        then smart_data_element_all.element_value
    end) as no_plan_reason
from {{ref('smart_data_element_all')}} as smart_data_element_all
inner join {{ref('stg_bh_cssrs_safety_plan_compliance')}} as stg_bh_cssrs_safety_plan_compliance
    on smart_data_element_all.visit_key = stg_bh_cssrs_safety_plan_compliance.visit_key
group by 1
),

safety_plan_patient as (
select
    smart_data_element_all.pat_key,
    max(case when smart_data_element_all.concept_id = 'CHOP#1624'
        then smart_data_element_all.entered_date
    end) as bh_safety_plan_text,
    max(case when smart_data_element_all.concept_id = 'EPIC#31000169363'
        then smart_data_element_all.entered_date
    end) as stanley_brown_update_pat,
    max(case when smart_data_element_all.concept_id = 'EPIC#31000169338'
        then smart_data_element_all.entered_date
    end) as stanley_brown_creation_pat
from {{ref('smart_data_element_all')}} as smart_data_element_all
inner join {{ref('stg_bh_cssrs_safety_plan_compliance')}} as stg_bh_cssrs_safety_plan_compliance
    on smart_data_element_all.pat_key = stg_bh_cssrs_safety_plan_compliance.pat_key
group by 1
)



select distinct
    stg_bh_cssrs_safety_plan_compliance.visit_key,
	stg_bh_cssrs_safety_plan_compliance.pat_key,
	stg_bh_cssrs_safety_plan_compliance.cssrs_entered_date,
	stg_bh_cssrs_safety_plan_compliance.mrn,
	stg_bh_cssrs_safety_plan_compliance.department_name,
	stg_bh_cssrs_safety_plan_compliance.initial_ed_department_center_abbr,
	stg_bh_cssrs_safety_plan_compliance.patient_class,
	stg_bh_cssrs_safety_plan_compliance.payor_group,
	stg_bh_cssrs_safety_plan_compliance.age_years,
	stg_bh_cssrs_safety_plan_compliance.provider_name,
	stg_bh_cssrs_safety_plan_compliance.hospital_admit_date,
	stg_bh_cssrs_safety_plan_compliance.hospital_discharge_date,
    stg_bh_cssrs_safety_plan_compliance.age_groups,
    stg_bh_cssrs_safety_plan_compliance.race_groups,
    stg_bh_cssrs_safety_plan_compliance.ethnicity,
    stg_bh_cssrs_safety_plan_compliance.sex,
    stg_bh_cssrs_safety_plan_compliance.svi_category,
    safety_plan_visit.bh_safety_plan_sde as bh_safety_plan_date,
    coalesce(
        safety_plan_visit.stanley_brown_update_visit, safety_plan_patient.stanley_brown_update_pat
    ) as stanley_brown_update_date,
    coalesce(
        safety_plan_visit.stanley_brown_creation_visit, safety_plan_patient.stanley_brown_creation_pat
    ) as stanley_brown_creation_date,
    safety_plan_patient.bh_safety_plan_text as suicide_safety_plan_date,
    cssrs_survey.entered_employee,
    case when date(bh_safety_plan_sde) <= hospital_discharge_date + 1
            or date(bh_safety_plan_text) <= hospital_discharge_date + 1
            or date(stanley_brown_update_date) <= hospital_discharge_date + 1
            or date(stanley_brown_creation_date) <= hospital_discharge_date + 1
            then 1 else 0
    end as safety_plan_ind,
    safety_plan_visit.no_plan_reason
from {{ref('stg_bh_cssrs_safety_plan_compliance')}} as stg_bh_cssrs_safety_plan_compliance
left join safety_plan_visit as safety_plan_visit
    on stg_bh_cssrs_safety_plan_compliance.visit_key = safety_plan_visit.visit_key
left join safety_plan_patient as safety_plan_patient
    on stg_bh_cssrs_safety_plan_compliance.pat_key = safety_plan_patient.pat_key
left join {{ref('cssrs_survey')}} as cssrs_survey
    on stg_bh_cssrs_safety_plan_compliance.visit_key = cssrs_survey.visit_key
where stg_bh_cssrs_safety_plan_compliance.hospital_admit_date >= '2020-07-01'
