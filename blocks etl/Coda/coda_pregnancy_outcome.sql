/* Pregnancy outcome results from Clinical Outcome Data Archival application. */

{{
    config(materialized = 'view')
}}

with merged_data_points as (
    select
        data_points.id,
        cards.code as card_code,
        card_instances.queue_id,
        case when data_points.response_id is null
            then trim(coalesce(validated_data_points.value_str,
                to_char(validated_data_points.value_date, 'YYYY-MM-DD HH:MI:SS'),
                to_char(validated_data_points.value_int, '999999999'),
                to_char(validated_data_points.value_dec, '999999999.99')))
            else
            trim(responses.name) end::varchar(100) as vdp_result_value,
        case when data_points.response_id is null
            then trim(coalesce(data_points.value_str,
                to_char(data_points.value_date, 'YYYY-MM-DD HH:MI:SS'),
                to_char(data_points.value_int, '999999999'),
                to_char(data_points.value_dec, '999999999.99')))
            else
            trim(responses.name) end::varchar(100) as dp_result_value,
		data_points.card_instance_id,
		data_points.patient_id,
		data_points.variable_id,
		validated_data_points.seq_num,
		validated_data_points.group_num,
		validated_data_points.nested_seq_num
    from {{ source('coda_ods', 'coda_data_points') }}  as data_points
    left join {{ source('coda_ods', 'coda_validated_data_points') }} as validated_data_points
        on validated_data_points.data_point_id = data_points.id
    left join {{ source('coda_ods', 'coda_responses') }} as responses
        on responses.id = data_points.response_id
    left join {{ source('coda_ods', 'coda_card_instances') }} as card_instances
        on data_points.card_instance_id = card_instances.id
    left join {{ source('coda_ods', 'coda_cards') }} as cards
        on cards.id = card_instances.card_id
    where card_instances.queue_id not in ('4')
        and card_instances.deleted_at is null
        and data_points.deleted_at is null
        and validated_data_points.deleted_at is null
        and cards.code in ('preg_episode', 'preg_delivery')
),
pregnancy_episode as (
	select
		patient_id,
		card_instance_id,
		card_code,
		max(case when variable_id = '156' then vdp_result_value end) as pregnancy_episode_id,
		max(case when variable_id = '158' then vdp_result_value end)::timestamp as estimated_date_of_delivery
	from merged_data_points
	where
		variable_id in ('156', '158') and card_code in ('preg_episode') and queue_id = 3
	group by
		patient_id,
		card_instance_id,
		card_code
),
pregnancy_delivery as (
	select
		patient_id,
		card_instance_id,
		card_code,
        max(case when variable_id = '181' then dp_result_value end)::timestamp as delivery_date,
        max(case when variable_id = '182' then dp_result_value end) as delivery_mode
	from merged_data_points
	where
		variable_id in ('181', '182', '164') and card_code in ('preg_delivery')
	group by
		patient_id,
		card_instance_id,
		card_code
)

select
    coda_patients.mrn,
    coda_patients.id,
    coda_patients.first_name,
    coda_patients.last_name,
    coda_patients.dob,
    preg_episode.pregnancy_episode_id,
    preg_episode.estimated_date_of_delivery,
    preg_episode.estimated_date_of_delivery + interval '30 day' as custom_estimated_date_of_delivery,
    case
        when preg_delivery.delivery_date is null
            and preg_episode.estimated_date_of_delivery + interval '30 day' < current_date then 1
        else 0
    end as expiry_ind,
    preg_delivery.delivery_date,
    preg_delivery.delivery_mode
from {{ source('coda_ods', 'coda_patients') }} as coda_patients
left join pregnancy_episode as preg_episode on preg_episode.patient_id = coda_patients.id
left join pregnancy_delivery as preg_delivery on preg_delivery.patient_id = coda_patients.id
    and preg_delivery.delivery_date between preg_episode.estimated_date_of_delivery - interval '40 week'
            and preg_episode.estimated_date_of_delivery + interval '5 week'
where preg_episode.estimated_date_of_delivery is not null
