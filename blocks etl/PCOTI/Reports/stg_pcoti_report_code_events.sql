with redcap_cat_codes as (
    select
        pcoti_episode_events.*,
        -- set default value for next_cat_code_date to extremely high date
        -- to ensure we never miss events
        lead(pcoti_episode_events.event_start_date, 1, '2999-12-31 23:59:59'::timestamp) over (
            partition by pcoti_episode_events.episode_key
            order by pcoti_episode_events.event_start_date
        ) as next_cat_code_date
    from
        {{ ref('pcoti_episode_events') }} as pcoti_episode_events
    where
        pcoti_episode_events.event_type_abbrev in (
            'REDCAP_CAT_CALL',
            'REDCAP_CODE_OTHER',
            'REDCAP_CODE_ARC',
            'REDCAP_CODE_ARC_CPA',
            'REDCAP_NONPAT_RESPTEAM',
            'REDCAP_CODE_CPA',
            'REDCAP_CODE_AA'
        )
),

redcap_codes as (
    select
        redcap_cat_codes.*
    from
        redcap_cat_codes
    where
        redcap_cat_codes.event_type_abbrev not in (
            'REDCAP_CAT_CALL'
        )
        and redcap_cat_codes.department_group_name not in (
            'NICU',
            'NICU OVF',
            'PICU',
            'PICU OVF',
            'CICU',
            'CICU OVF',
            'ED',
            'EDECU',
            'PCU',
            'CPRU'
        )
),

first_post_code_icu_transfers as (
    select
        inner_qry.*
    from (
        select
            redcap_codes.*,
            pcoti_icu_transfers.icu_enter_date,
            row_number() over(
                partition by redcap_codes.episode_key, redcap_codes.episode_event_key
                order by pcoti_icu_transfers.icu_enter_date
            ) as icu_xfer_seq
        from
            redcap_codes
            inner join {{ ref('pcoti_icu_transfers') }} as pcoti_icu_transfers
                on redcap_codes.episode_key = pcoti_icu_transfers.episode_key
                and pcoti_icu_transfers.icu_enter_date >= redcap_codes.event_start_date
                and pcoti_icu_transfers.icu_enter_date <= redcap_codes.event_start_date + interval '24 hours'
                and pcoti_icu_transfers.icu_enter_date <= redcap_codes.next_cat_code_date + interval '24 hours'
    ) as inner_qry
    where
        inner_qry.icu_xfer_seq = 1
),

post_code_icu_transfer_details as (
    select
        first_post_code_icu_transfers.episode_key,
        first_post_code_icu_transfers.episode_event_key,
        case
            when first_post_code_icu_transfers.icu_enter_date is null then 'No'
            else 'Yes'
        end as post_code_icu_xfer_ind,
        first_post_code_icu_transfers.icu_enter_date,
        (
            date_part('epoch', first_post_code_icu_transfers.icu_enter_date)
            - date_part('epoch', first_post_code_icu_transfers.event_start_date)
        ) as total_seconds_to_icu_xfer,
        floor(total_seconds_to_icu_xfer / 3600) as hours_to_xfer,
        floor(total_seconds_to_icu_xfer % 3600 / 60) as remainder_minutes_to_xfer,
        case
            when hours_to_xfer > 1 then hours_to_xfer::varchar(10) || ' hrs '
            when hours_to_xfer = 1 then hours_to_xfer::varchar(10) || ' hr '
            else ''
        end
        || case
            when remainder_minutes_to_xfer > 1 then remainder_minutes_to_xfer::varchar(5) || ' mins'
            when remainder_minutes_to_xfer = 1 then remainder_minutes_to_xfer::varchar(5) || ' min'
            else '0 mins'
        end as time_to_xfer
    from
        first_post_code_icu_transfers
)

select
    redcap_codes.episode_key,
    redcap_codes.episode_event_key,
    redcap_codes.pat_key,
    redcap_codes.visit_key,
    pcoti_cat_code_details_1.code_category,
    pcoti_cat_code_details_4.non_patient_category,
    redcap_codes.event_type_name,
    redcap_codes.event_type_abbrev,
    redcap_codes.event_start_date,
    stg_patient.mrn,
    encounter_inpatient.csn,
    coalesce(
        stg_patient.patient_name,
        initcap(pcoti_cat_code_details_1.last_name || ', ' || pcoti_cat_code_details_1.first_name)
    ) as patient_name,
    coalesce(stg_patient.dob, pcoti_cat_code_details_1.dob) as patient_dob,
    redcap_codes.ip_service_name,
    redcap_codes.dept_key,
    redcap_codes.department_name,
    redcap_codes.department_group_name,
    redcap_codes.bed_care_group,
    redcap_codes.campus_name,
    case
        when pcoti_cat_code_details_1.cha_code_ind = 1 then 'Yes'
        else 'No'
    end as cha_code_ind,
    case
        when pcoti_cat_code_details_1.cat_called_in_prior_24hrs_ind = 1 then 'Yes'
        else 'No'
    end as cat_called_in_prior_24hrs_ind,
    pcoti_cat_code_details_3.immediate_disposition,
    pcoti_cat_code_details_4.survival_status,
    pcoti_cat_code_details_3.dx_at_event,
    case
        when pcoti_episodes.pat_died_during_episode_ind = 1 then 'Yes'
        else 'No'
    end as pat_died_during_episode_ind,
    case
        when pcoti_episodes.pat_died_during_episode_ind = 1 then stg_patient.death_date
        else null
    end as patient_death_date,
    post_code_icu_transfer_details.icu_enter_date,
    post_code_icu_transfer_details.time_to_xfer
from
    redcap_codes
    inner join {{ ref('pcoti_episodes') }} as pcoti_episodes
        on redcap_codes.episode_key = pcoti_episodes.episode_key
    left join {{ ref('stg_patient') }} as stg_patient
        on redcap_codes.pat_key = stg_patient.pat_key
    left join {{ ref('encounter_inpatient') }} as encounter_inpatient
        on redcap_codes.visit_key = encounter_inpatient.visit_key
    inner join {{ ref('pcoti_cat_code_details_1' )}} as pcoti_cat_code_details_1
        on redcap_codes.episode_event_key = pcoti_cat_code_details_1.episode_event_key
    inner join {{ ref('pcoti_cat_code_details_3' )}} as pcoti_cat_code_details_3
        on redcap_codes.episode_event_key = pcoti_cat_code_details_3.episode_event_key
    inner join {{ ref('pcoti_cat_code_details_4' )}} as pcoti_cat_code_details_4
        on redcap_codes.episode_event_key = pcoti_cat_code_details_4.episode_event_key
    left join post_code_icu_transfer_details
        on redcap_codes.episode_key = post_code_icu_transfer_details.episode_key
        and redcap_codes.episode_event_key = post_code_icu_transfer_details.episode_event_key
