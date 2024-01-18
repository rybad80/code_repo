with participant_records as (
    select
        {{ dbt_utils.surrogate_key(['redcap_survey_participants.participant_id']) }}
            as redcap_participant_key,
        redcap_survey_participants.participant_id,
        redcap_survey_participants.survey_id,
        redcap_survey_participants.event_id,
        redcap_survey_participants.hash,
        redcap_survey_participants.legacy_hash,
        redcap_survey_participants.access_code,
        redcap_survey_participants.access_code_numeral,
        redcap_survey_participants.participant_email,
        redcap_survey_participants.participant_identifier,
        redcap_survey_participants.participant_phone,
        redcap_survey_participants.link_expiration,
        redcap_survey_participants.link_expiration_override
    from
        {{ source('ods_redcap_porter', 'redcap_surveys_participants') }}
            as redcap_survey_participants
),

record_union as (
    select 
        redcap_participant_key,
        participant_id,
        survey_id,
        event_id,
        hash,
        legacy_hash,
        access_code,
        access_code_numeral,
        participant_email,
        participant_identifier,
        participant_phone,
        link_expiration,
        link_expiration_override
    from 
        participant_records

    union all

    select 
        -1 as redcap_participant_key,
        null as participant_id,
        null as survey_id,
        null as event_id,
        null as hash,
        null as legacy_hash,
        null as access_code,
        null as access_code_numeral,
        null as participant_email,
        null as participant_identifier,
        null as participant_phone,
        null as link_expiration,
        null as link_expiration_override
)

select
    redcap_participant_key,
    participant_id,
    survey_id,
    event_id,
    hash,
    legacy_hash,
    access_code,
    access_code_numeral,
    participant_email,
    participant_identifier,
    participant_phone,
    link_expiration,
    link_expiration_override,
    'REDCAP~' || participant_id as integration_id,
    current_timestamp as create_date,
    'REDCAP' as create_source,
    current_timestamp as update_date,
    'REDCAP' as update_source
from
    record_union
