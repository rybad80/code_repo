{{ config(meta = {
    'critical': true
}) }}


select
ord_spec_quest.order_id,
ord_spec_quest.line as order_line,
ord_spec_quest.ord_quest_id as order_question_id,
dim_question.question_name as order_question_name,
cast(ord_spec_quest.ord_quest_resp as varchar(200)) as order_question_response,
ord_spec_quest.ord_quest_date as order_question_date
from  {{source('clarity_ods', 'ord_spec_quest')}}   as ord_spec_quest
left join {{ref('dim_question')}} as dim_question
    on ord_spec_quest.ord_quest_id = dim_question.question_id
where exists (
        select stg_procedure_order_encounter.procedure_order_id
        from {{ref('stg_procedure_order_encounter')}} as stg_procedure_order_encounter
        where
         ord_spec_quest.order_id = stg_procedure_order_encounter.procedure_order_id)
     