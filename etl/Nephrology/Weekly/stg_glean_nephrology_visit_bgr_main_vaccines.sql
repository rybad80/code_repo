with visit_bgr_main_vaccines as ( -- calculates indicators for vaccines received at BGR/Main Nephrology,
                                  -- in 18 months preceding most recent nephrology appointment
    select
        patient_nephrology_visits.visit_key,
        patient_nephrology_visits.pat_key,
        patient_nephrology_visits.mrn,
        patient_nephrology_visits.encounter_date,
        patient_nephrology_visits.department_name,
        max(case when lower(procedure_order_clinical.procedure_group_name) = 'immunization/injection'
                 and lower(procedure_order_clinical.procedure_name) like '%prevnar%' then 1 else 0
                 end) as prevnar_main_ind,
        max(case when lower(procedure_order_clinical.procedure_group_name) = 'immunization/injection'
                 and lower(procedure_order_clinical.procedure_name) like '%pneumoccoccal%'
                 and lower(procedure_order_clinical.procedure_name) not like '%prevnar%' then 1 else 0
                 end) as pneumovax_main_ind,
        max(case when lower(procedure_order_clinical.procedure_group_name) = 'immunization/injection'
                 and lower(procedure_order_clinical.procedure_name) like '%flu%'
                 and lower(procedure_order_clinical.procedure_name) not like '%h1n1%' then 1 else 0
                 end) as flu_main_ind,
        max(case when lower(procedure_order_clinical.procedure_group_name) = 'immunization/injection'
                 and lower(procedure_order_clinical.procedure_name) like '%covid%' then 1 else 0
                 end) as covid_19_main_ind
    from
        {{ ref('stg_glean_nephrology_patient_nephrology_visits')}} as patient_nephrology_visits
    left join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
            on patient_nephrology_visits.visit_key = procedure_order_clinical.visit_key
    where
        patient_nephrology_visits.department_id in (101012142,  -- BGR Nephrology
                                                    89375022)   -- Main Nephrology
        and lower(procedure_order_clinical.order_status) = 'completed'
    group by
        patient_nephrology_visits.visit_key,
        patient_nephrology_visits.pat_key,
        patient_nephrology_visits.mrn,
        patient_nephrology_visits.encounter_date,
        patient_nephrology_visits.department_name
)

select
    *
from
    visit_bgr_main_vaccines
