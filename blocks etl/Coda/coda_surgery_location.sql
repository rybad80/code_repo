/* Surgery location from Clinical Outcome Data Archival application merged with CDW*/

{{
    config(materialized = 'view')
}}

with merged_data_points as (
    select
        data_points.id,
        cards.code as card_code,
        card_instances.queue_id,
        patients.mrn as patient_mrn,
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
    left join {{ source('coda_ods', 'coda_patients') }} as patients
        on data_points.patient_id = patients.id
    where
        card_instances.deleted_at is null
        and data_points.deleted_at is null
        and validated_data_points.deleted_at is null
        and patients.deleted_at is null
		and card_instances.queue_id in ('9', '57', '31', '14')
        and cards.code in ('cdh_repair', 'core_fundo',
            'g_tube', 'll_rxn_surgery', 'omph_surg_closure', 'trac_creat')
),
surgery_location as (
    select
        patient_mrn,
        card_instance_id,
        card_code,
        max(case when variable_id = '55' then vdp_result_value end) as coda_procedure_location,
        max(case when variable_id = '28' then vdp_result_value end) as coda_procedure_name,
        max(case when variable_id = '27' then vdp_result_value end) as coda_date_of_procedure
    from
        merged_data_points
    where
        variable_id in ('28', '27', '55')
    group by
        patient_mrn,
        card_instance_id,
        card_code
),
surgery_keys as (
    select
        card_instance_id,
        max(value) as or_key
    from {{ source('coda_ods', 'coda_card_instance_key_values') }}
    where
        lower(unique_field) = 'or_key'
    group by
        card_instance_id
)
select
    surg_loc.patient_mrn as mrn,
    surg_loc.card_instance_id,
    surg_loc.card_code,
    surg_loc.coda_procedure_location,
    surg_loc.coda_procedure_name,
    surg_loc.coda_date_of_procedure,
    surg_enc.room as cdw_room,
    surg_enc.location as cdw_location,
    surg_enc.location_group as cdw_location_group
from surgery_location as surg_loc
left join surgery_keys as surg_keys on surg_loc.card_instance_id = surg_keys.card_instance_id
left join {{ ref('surgery_encounter') }} as surg_enc on surg_keys.or_key = surg_enc.or_key
    where surg_keys.or_key is not null
