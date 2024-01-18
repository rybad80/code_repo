select
    stg_preference_cards.preference_card_key,
    stg_preference_cards.or_proc_id,
    stg_preference_cards.or_procedure_name,
    stg_preference_cards.or_procedure_active_ind,
    stg_preference_cards.preference_card_id,
    stg_preference_cards.preference_card_name,
    stg_preference_cards.preference_card_active_ind,
    stg_preference_cards.default_or_modified_preference_card,
    stg_preference_cards.picklist_id,
    stg_preference_cards.provider_name,
    stg_preference_cards.provider_id,
    stg_preference_cards.location_name,
    stg_preference_cards.location_id,
    stg_preference_cards.last_reviewed_date
from
    {{ref('stg_preference_cards')}} as stg_preference_cards
