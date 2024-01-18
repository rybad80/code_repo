/* HFOV ventilation assitance results from Clinical Outcome Data Archival application. */

{{
    config(materialized = 'view')
}}

with merged_data_points as (
    select
        validated_data_points.id,
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
        variables.code as variable_code,
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
    left join  {{ source('coda_ods', 'coda_variables') }} as variables
        on variables.id = data_points.variable_id
    where card_instances.queue_id = 41
        and card_instances.deleted_at is null
        and data_points.deleted_at is null
        and validated_data_points.deleted_at is null
        and lower(cards.code) in ('core_sduresus', 'core_sduresus_v2')
),
discharge_summary as (
	select
        patient_id,
        card_instance_id,
        card_code,
        seq_num,
        group_num,
        max(case when variable_code = 'core_sduresus_vitalstime'
                then vdp_result_value end)::timestamp as vitals_time,
        max(case when variable_code in ( 'core_sduresus_ventassistance', 'core_sduresus_respsupp')
                then vdp_result_value end) as vent_assistance
    from merged_data_points
    where
        lower(card_code) in ('core_sduresus', 'core_sduresus_v2') and queue_id = 41
        and variable_code in ('core_sduresus_vitalstime', 'core_sduresus_ventassistance', 'core_sduresus_respsupp')
    group by
        patient_id,
        card_instance_id,
        card_code,
        seq_num,
        group_num,
        nested_seq_num
),
ventilation_assistance_hfov as (
        select
            patient_id,
            card_instance_id,
            vent_assistance,
            min(vitals_time) as vitals_time
        from discharge_summary
        where lower(vent_assistance) = 'hfov'
            and vitals_time is not null
        group by
            patient_id,
            card_instance_id,
            vent_assistance
)

select
    coda_patients.mrn,
    coda_patients.id,
    coda_patients.first_name,
    coda_patients.last_name,
    coda_patients.dob,
    vent_assist_hfov.vitals_time,
    vent_assist_hfov.vent_assistance
from {{ source('coda_ods', 'coda_patients') }} as coda_patients
inner join ventilation_assistance_hfov as vent_assist_hfov on vent_assist_hfov.patient_id = coda_patients.id
