with base_orders as (

select
order_proc.proc_id,
referral_order_id.referral_id,
ord_spec_quest.order_id,
ord_spec_quest.ord_quest_date,
ord_spec_quest.ord_quest_id,
cl_qquest.quest_name as question_name,
cast(ord_spec_quest.ord_quest_resp as varchar(200)) as response
from {{source('clarity_ods', 'ord_spec_quest')}} as ord_spec_quest
inner join  {{source('clarity_ods', 'referral_order_id')}} as referral_order_id
	on ord_spec_quest.order_id = referral_order_id.order_id
inner join {{source('clarity_ods', 'order_proc')}}  as order_proc
     on order_proc.order_proc_id  = referral_order_id.order_id
 left join {{source('clarity_ods', 'cl_qquest')}} as cl_qquest
     on ord_spec_quest.ord_quest_id = cl_qquest.quest_id
where  ord_spec_quest.ord_quest_id in ('127701', '127649', '128070', '127654', '127656', '127657',
'113723', '113724', '113725', '113727', '113728', '113820', '120192', '120193', '120194', '120195', '121367',
'121375', '121377', '121379', '121480', '122032', '122040', '122041', '122456', '122504', '122506', '122507',
'122508', '122510', '127527', '127529', '127533', '127574', '127589', '127593', '127594', '127595', '127597',
'128035', '128403', '128404', '128744', '128745', '128747', '128749', '128750', '128751', '128752', '128895',
'128896', '128897', '128898', '128899', '128986', '128987', '129450', '131469', '131470', '131472', '131473',
'131474', '131475', '131476', '131478', '131479', '131480', '131481', '131482', '131483', '131493', '131684',
'131687', '132127', '148636', '148639', '148791', '148792', '148794')
 and order_proc.proc_id in (119530, 100619, 108884, 127367, 96871, 127576, 127577, 127613, 127676,
101981, 119366, 129706, 129704, 129295, 129294, 129708, 129296 )
)


select
	referral_id,
	order_id,
	proc_id as procedure_id,
	ord_quest_date as order_question_date,
	question_name,
	ord_quest_id as order_question_id,
	cast(group_concat(response, '*') as varchar(200))  as response
from base_orders
group by referral_id, proc_id, order_id, question_name, ord_quest_date, ord_quest_id
