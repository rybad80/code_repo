select
    stg_pcoti_transfer_review_redcap_all.pat_key,
    coalesce(
        encounter_inpatient.visit_key,
        stg_pcoti_transfer_review_redcap_all.visit_key
    ) as visit_key,
    'Transfer Review - Huddle' as event_type_name,
    'TRAN_REVIEW_HUDDLE' as event_type_abbrev,
    stg_pcoti_transfer_review_redcap_all.huddle_date as event_start_date,
    null as event_end_date
from
    {{ ref('stg_pcoti_transfer_review_redcap_all') }} as stg_pcoti_transfer_review_redcap_all
    inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on stg_pcoti_transfer_review_redcap_all.pat_key = encounter_inpatient.pat_key
        and stg_pcoti_transfer_review_redcap_all.huddle_date >= encounter_inpatient.hospital_admit_date
        and stg_pcoti_transfer_review_redcap_all.huddle_date <= encounter_inpatient.hospital_discharge_date
where
    stg_pcoti_transfer_review_redcap_all.huddle_date >= '2017-01-01'

union all

select
    stg_pcoti_transfer_review_redcap_all.pat_key,
    coalesce(
        encounter_inpatient.visit_key,
        stg_pcoti_transfer_review_redcap_all.visit_key
    ) as visit_key,
    'Transfer Review - Tier II' as event_type_name,
    'TRAN_REVIEW_TIER_II' as event_type_abbrev,
    stg_pcoti_transfer_review_redcap_all.tier_ii_review_date as event_start_date,
    null as event_end_date
from
    {{ ref('stg_pcoti_transfer_review_redcap_all') }} as stg_pcoti_transfer_review_redcap_all
    inner join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on stg_pcoti_transfer_review_redcap_all.pat_key = encounter_inpatient.pat_key
        and stg_pcoti_transfer_review_redcap_all.tier_ii_review_date >= encounter_inpatient.hospital_admit_date
        and stg_pcoti_transfer_review_redcap_all.tier_ii_review_date <= encounter_inpatient.hospital_discharge_date
where
    stg_pcoti_transfer_review_redcap_all.tier_ii_review_date >= '2017-01-01'
