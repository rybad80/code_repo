with episodes as (
    select
        stg_pcoti_event_all.episode_key,
        stg_pcoti_event_all.pat_key,
        max(stg_pcoti_event_all.visit_key) as visit_key,
        max(stg_pcoti_event_all.redcap_record_id) as redcap_record_id,
        encounter_inpatient.hospital_admit_date,
        encounter_inpatient.hospital_discharge_date,
        case
            when encounter_inpatient.hospital_discharge_date is null
            and encounter_inpatient.hospital_admit_date is not null
            then 1
            else 0
        end as active_episode_ind,
        case
            when stg_patient.death_date >= encounter_inpatient.hospital_admit_date
            and stg_patient.death_date <= encounter_inpatient.hospital_discharge_date then 1
            else 0
        end as pat_died_during_episode_ind,
        encounter_inpatient.admission_department_center_abbr,
        max(
            case
                when stg_pcoti_event_all.event_type_abbrev in (
                    'NOTE_CAT_INIT',
                    'NOTE_CAT_EVAL',
                    'REDCAP_CAT_CALL'
                ) then 1
                else 0
            end
        ) as cat_call_ind,
        max(
            case
                when stg_pcoti_event_all.event_type_abbrev in (
                    'REDCAP_CODE_OTHER',
                    'REDCAP_CODE_ARC',
                    'REDCAP_CODE_ARC_CPA',
                    'REDCAP_CODE_CPA',
                    'REDCAP_CODE_AA',
                    'REDCAP_NONPAT_RESPTEAM'
                ) then 1
                else 0
            end
        ) as code_ind,
        max(
            case
                when stg_pcoti_event_all.event_type_abbrev in (
                    'ICU_XFER_EMERGENT_UNPLANNED'
                ) then 1
                else 0
            end
        ) as emergent_transfer_ind,
        max(
            case
                when stg_pcoti_event_all.event_type_abbrev in (
                    'NOTE_CCOT', 'NOTE_CCOT_RN_RT'
                ) then 1
                else 0
            end
        ) as ccot_ind,
        max(
            case
                when stg_pcoti_event_all.event_type_abbrev = 'WATCHER' then 1
                else 0
            end
        ) as watcher_ind,
        max(
            case
                when stg_pcoti_event_all.event_type_abbrev = 'TRAN_REVIEW_HUDDLE' then 1
                else 0
            end
        ) as tran_review_huddle_ind,
        max(
            case
                when stg_pcoti_event_all.event_type_abbrev = 'TRAN_REVIEW_TIER_II' then 1
                else 0
            end
        ) as tran_review_tier_ii_ind,
        max(
            case
                when stg_pcoti_event_all.event_type_abbrev like 'VASOPRESSOR%' then 1
                else 0
            end
        ) as vasopressor_ind,
        max(
            case
                when stg_pcoti_event_all.event_type_abbrev = 'FLUID_BOLUS_GT60' then 1
                else 0
            end
        ) as fluid_bolus_gt60_ind,
        max(
            case
                when stg_pcoti_event_all.event_type_abbrev = 'INTUB' then 1
                else 0
            end
        ) as intubation_ind
    from
        {{ ref('stg_pcoti_event_all') }} as stg_pcoti_event_all
        left join {{ ref('encounter_inpatient') }} as encounter_inpatient
            on stg_pcoti_event_all.visit_key = encounter_inpatient.visit_key
        left join {{ ref('stg_patient') }} as stg_patient
            on stg_pcoti_event_all.pat_key = stg_patient.pat_key
    group by
        stg_pcoti_event_all.episode_key,
        stg_pcoti_event_all.pat_key,
        encounter_inpatient.hospital_admit_date,
        encounter_inpatient.hospital_discharge_date,
        encounter_inpatient.admission_department_center_abbr,
        stg_patient.death_date
)

select
    episodes.episode_key,
    episodes.pat_key,
    episodes.visit_key,
    case
        when episodes.visit_key is null
        or episodes.visit_key = 0
        then episodes.redcap_record_id
        else null
    end as redcap_record_id,
    episodes.hospital_admit_date,
    episodes.hospital_discharge_date,
    episodes.admission_department_center_abbr,
    episodes.active_episode_ind,
    episodes.pat_died_during_episode_ind,
    episodes.cat_call_ind,
    episodes.code_ind,
    episodes.emergent_transfer_ind,
    episodes.ccot_ind,
    episodes.watcher_ind,
    episodes.tran_review_huddle_ind,
    episodes.tran_review_tier_ii_ind,
    episodes.vasopressor_ind,
    episodes.fluid_bolus_gt60_ind,
    episodes.intubation_ind
from
    episodes
where
    episodes.cat_call_ind
    + episodes.code_ind
    + episodes.emergent_transfer_ind > 0
