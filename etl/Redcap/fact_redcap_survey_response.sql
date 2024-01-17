with response_records as (
    select
        {{ dbt_utils.surrogate_key(['redcap_surveys_response.response_id'])}}
            as redcap_response_key,
        redcap_surveys_response.response_id,
        redcap_surveys_response.participant_id,
        redcap_surveys_response.record as record_id,
        redcap_surveys_response.instance as record_instance_num,
        redcap_surveys_response.start_time,
        redcap_surveys_response.first_submit_time,
        redcap_surveys_response.completion_time,
        redcap_surveys_response.return_code,
        redcap_surveys_response.results_code
    from
        {{ source('ods_redcap_porter', 'redcap_surveys_response') }}
            as redcap_surveys_response
)

select
    response_records.redcap_response_key,
    response_records.response_id,
    coalesce(
        dim_redcap_participant.redcap_participant_key, 
        -1
    ) as redcap_participant_key,
    response_records.participant_id,
    response_records.record_id,
    response_records.record_instance_num,
    response_records.start_time,
    response_records.first_submit_time,
    response_records.completion_time,
    response_records.return_code,
    response_records.results_code
from
    response_records
    left join {{ ref('dim_redcap_participant') }} as dim_redcap_participant
        on
            response_records.participant_id
            = dim_redcap_participant.participant_id
