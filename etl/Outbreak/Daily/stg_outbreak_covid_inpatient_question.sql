{{ config(meta = {
    'critical': true
}) }}


select
    procedure_order_clinical.visit_key,
    max(case when stg_order_questions.order_question_id = '500500658'
            then stg_order_questions.order_question_response
        end ) as isolation_desc,
    max(case when stg_order_questions.order_question_id = '500500664'
            and stg_order_questions.order_question_response = 'Yes' then  1 else 0
        end ) as covid19_exposure_ind,
    max(case when stg_order_questions.order_question_id = '500500665'
             and stg_order_questions.order_question_response = 'Yes' then 1 else 0
        end ) as fever_last_24_hours_ind,
    max( case when stg_order_questions.order_question_id = '500500666'
             and stg_order_questions.order_question_response = 'Yes' then 1 else 0
        end ) as respiratory_symptoms_ind,
    max(case when stg_order_questions.order_question_id = '500500667'
             and stg_order_questions.order_question_response = 'Yes' then 1 else 0
        end) as aerosol_generating_procedure_ind,
    max(case when stg_order_questions.order_question_id = '500500668'
             and stg_order_questions.order_question_response = 'Yes' then 1 else 0
        end) as mis_c_concern_ind,
    case when procedure_order_clinical.placed_date = max(
        procedure_order_clinical.placed_date) over (partition by procedure_order_clinical.visit_key) then 1
    end as last_order
from
    {{ref('procedure_order_clinical')}}  as procedure_order_clinical
    inner join {{ref('stg_order_questions')}} as stg_order_questions
        on procedure_order_clinical.procedure_order_id  = stg_order_questions.order_id
where
    stg_order_questions.order_question_id in ('500500658',  -- Isolation
                                 '500500664',  -- COVID-19 Exposure
                                 '500500665',  -- Fever Last 24 Hours
                                 '500500666',  -- Respiratory Symptoms
                                 '500500667',  -- Aerosol Generating Procedure
                                 '500500668'  -- MIS-C Concern
                                )
    and procedure_order_clinical.placed_date >= '2020-08-17 08:00:00'  --when questions were added to order form
group by
    procedure_order_clinical.visit_key,
    procedure_order_clinical.placed_date
