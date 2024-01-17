with patient_visits as (
    select
        pat_key,
        visit_key,
        visit_date,
        chop_market,
        age_years,
        row_number() over(partition by pat_key order by pat_key, visit_date) as visit_number,
        lag(visit_date) over(partition by pat_key order by pat_key, visit_date) as prev_visit,
        case
            when extract(epoch from visit_date - prev_visit) / (365.25) >= 3 then 1 --noqa: PRS
            when visit_number = 1 then 1
                else 0
            end as new_cancer_center_patient_ind
    from
        {{ref ('stg_cancer_center_visit')}}
)

select
    date_trunc('month', patient_visits.visit_date) as visual_month,
    patient_visits.visit_key,
    patient_visits.visit_date,
    patient_visits.new_cancer_center_patient_ind,
    cancer_center_patient.mrn,
    cancer_center_patient.pat_key,
    patient_visits.chop_market,
    patient_visits.age_years,
    case when cancer_center_patient.touch_category = 'base cohort' then 'base cohort w/ visit'
        else cancer_center_patient.touch_category end as touch_category,
    cancer_center_patient.death_date,
    date_trunc('month', cancer_center_patient.death_date) as death_date_visual_month
from
    {{ref ('cancer_center_patient')}} as cancer_center_patient
    inner join patient_visits
        on cancer_center_patient.pat_key = patient_visits.pat_key
