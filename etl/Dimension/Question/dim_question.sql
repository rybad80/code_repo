{{ config(meta = {
    'critical': true
}) }}

with latest_questions as (
select
	quest_id,
	max(contact_date_real) as latest_question
from {{source('clarity_ods', 'cl_qquest_ovtm')}}
group by quest_id
)


select
    {{dbt_utils.surrogate_key([
            'cl_qquest.quest_id'
        ])
    }} as question_key,
cl_qquest.quest_id as question_id,
cl_qquest.quest_name as question_name,
cl_qquest_ovtm.question::varchar(200) as display_name, 
cl_qquest_ovtm.contact_date as contact_date,
current_timestamp as create_date
from  {{source('clarity_ods', 'cl_qquest')}} as cl_qquest 
left join latest_questions as latest_questions on
    latest_questions.quest_id = cl_qquest.quest_id
inner join   {{source('clarity_ods', 'cl_qquest_ovtm')}}  as cl_qquest_ovtm on
     latest_questions.quest_id = cl_qquest_ovtm.quest_id
     and latest_questions.latest_question = cl_qquest_ovtm.contact_date_real