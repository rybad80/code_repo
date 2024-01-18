-- get all CAT call records from redcap
with redcap_cat_calls as (
    select
        pcoti_episode_events.*,
        lead(pcoti_episode_events.event_start_date, 1) over(
            partition by pcoti_episode_events.episode_key
            order by pcoti_episode_events.event_start_date
        ) as next_cat_record_date,
        case
            when next_cat_record_date is null
            or (
                date_part('epoch', next_cat_record_date - pcoti_episode_events.event_start_date)
                / 3600.0
            ) > 1 then 1
            else 0
        end as gt_1hr_to_next_cat,
        pcoti_episode_events.event_start_date - interval '24 hours' as redcap_entry_pre_24hrs,
        pcoti_episode_events.event_start_date + interval '24 hours' as redcap_entry_post_24hrs
    from
        {{ ref('pcoti_episode_events') }} as pcoti_episode_events
    where
        pcoti_episode_events.event_type_abbrev = 'REDCAP_CAT_CALL'
),

-- filter out repeat CAT records (or double entry)
non_repeat_redcap_cat_calls as (
    select
        redcap_cat_calls.*
    from
        redcap_cat_calls
    where
        redcap_cat_calls.gt_1hr_to_next_cat = 1
),

-- get all CAT init notes
cat_init_notes as (
    select
        pcoti_episode_events.*
    from
        {{ ref('pcoti_episode_events') }} as pcoti_episode_events
    where
        pcoti_episode_events.event_type_abbrev = 'NOTE_CAT_INIT'
),

-- get cat init notes not within +/- 24hrs of a redcap CAT record
lone_cat_init_notes as (
    select
        cat_init_notes.*
    from
        cat_init_notes
        full outer join non_repeat_redcap_cat_calls
            on cat_init_notes.episode_key = non_repeat_redcap_cat_calls.episode_key
            and cat_init_notes.event_start_date > non_repeat_redcap_cat_calls.redcap_entry_pre_24hrs
            and cat_init_notes.event_start_date < non_repeat_redcap_cat_calls.redcap_entry_post_24hrs
    where
        non_repeat_redcap_cat_calls.episode_key is null
),

-- get all other cat init notes for linking to redcap records
non_lone_cat_init_notes as (
    select
        cat_init_notes.*
    from
        cat_init_notes
        left join lone_cat_init_notes
            on cat_init_notes.episode_event_key = lone_cat_init_notes.episode_event_key
    where
        lone_cat_init_notes.episode_event_key is null
),

-- join notes to redcap records
matched_redcap_cats as (
    select
        non_repeat_redcap_cat_calls.episode_key,
        non_repeat_redcap_cat_calls.episode_event_key as redcap_episode_event_key,
        non_repeat_redcap_cat_calls.redcap_record_id,
        non_repeat_redcap_cat_calls.pat_key,
        non_repeat_redcap_cat_calls.visit_key,
        non_repeat_redcap_cat_calls.event_type_name,
        non_repeat_redcap_cat_calls.event_start_date as redcap_record_date,
        non_repeat_redcap_cat_calls.ip_service_name,
        non_repeat_redcap_cat_calls.dept_key,
        non_repeat_redcap_cat_calls.department_name,
        non_repeat_redcap_cat_calls.department_group_name,
        non_repeat_redcap_cat_calls.campus_name,
        row_number() over (
            partition by non_repeat_redcap_cat_calls.episode_key, non_repeat_redcap_cat_calls.episode_event_key
            order by non_lone_cat_init_notes.event_start_date
        ) as note_seq,
        non_lone_cat_init_notes.episode_event_key as init_note_episode_event_key,
        non_lone_cat_init_notes.event_start_date as init_note_date
    from
        non_repeat_redcap_cat_calls
        left join non_lone_cat_init_notes
            on non_repeat_redcap_cat_calls.episode_key = non_lone_cat_init_notes.episode_key
            and non_lone_cat_init_notes.event_start_date >= non_repeat_redcap_cat_calls.event_start_date
            and non_lone_cat_init_notes.event_start_date <= non_repeat_redcap_cat_calls.redcap_entry_post_24hrs
),

-- filter down to only first matching CAT note
-- there are cases where a single note is matched to more than 1 redcap record
first_match_redcap_cat as (
    select
        matched_redcap_cats.episode_key,
        matched_redcap_cats.pat_key,
        matched_redcap_cats.visit_key,
        matched_redcap_cats.event_type_name,
        matched_redcap_cats.redcap_episode_event_key,
        matched_redcap_cats.redcap_record_id,
        matched_redcap_cats.redcap_record_date,
        matched_redcap_cats.ip_service_name,
        matched_redcap_cats.dept_key,
        matched_redcap_cats.department_name,
        matched_redcap_cats.department_group_name,
        matched_redcap_cats.campus_name,
        matched_redcap_cats.init_note_episode_event_key,
        matched_redcap_cats.init_note_date
    from
        matched_redcap_cats
    where
        matched_redcap_cats.note_seq = 1
),

-- put non-redcap CAT records into correct format for union
non_redcap_cats as (
    select
        lone_cat_init_notes.episode_key,
        lone_cat_init_notes.pat_key,
        lone_cat_init_notes.visit_key,
        lone_cat_init_notes.event_type_name,
        null as redcap_episode_event_key,
        null as redcap_record_id,
        null as redcap_record_date,
        lone_cat_init_notes.ip_service_name,
        lone_cat_init_notes.dept_key,
        lone_cat_init_notes.department_name,
        lone_cat_init_notes.department_group_name,
        lone_cat_init_notes.campus_name,
        lone_cat_init_notes.episode_event_key as init_note_episode_event_key,
        lone_cat_init_notes.event_start_date as init_note_date
    from
        lone_cat_init_notes
),

-- combine all CATs from both sources
all_cats as (
    select * from first_match_redcap_cat
    union all
    select * from non_redcap_cats
),

-- add in other CAT details from redcap
all_cats_w_details as (
    select
        all_cats.episode_key,
        all_cats.pat_key,
        all_cats.visit_key,
        coalesce(
            all_cats.redcap_episode_event_key,
            all_cats.init_note_episode_event_key
        ) as index_episode_event_key,
        all_cats.event_type_name as index_source_event_type,
        coalesce(all_cats.redcap_record_date, all_cats.init_note_date) as index_event_date,
        stg_patient.mrn,
        encounter_inpatient.csn,
        coalesce(
            stg_patient.patient_name,
            initcap(pcoti_cat_code_details_1.last_name || ', ' || pcoti_cat_code_details_1.first_name)
        ) as patient_name,
        coalesce(stg_patient.dob, pcoti_cat_code_details_1.dob) as patient_dob,
        all_cats.redcap_episode_event_key,
        all_cats.redcap_record_id,
        all_cats.redcap_record_date,
        all_cats.ip_service_name,
        all_cats.dept_key,
        all_cats.department_name,
        all_cats.department_group_name,
        all_cats.campus_name,
        pcoti_cat_code_details_3.dx_at_event,
        pcoti_cat_code_details_3.immediate_disposition,
        pcoti_cat_code_details_4.survival_status,
        pcoti_cat_code_details_1.cat_provider_type,
        all_cats.init_note_episode_event_key,
        all_cats.init_note_date,
        stg_pcoti_notes.note_key as init_note_key,
        stg_pcoti_notes.note_id as init_note_id
    from
        all_cats
        left join {{ ref('stg_pcoti_notes') }} as stg_pcoti_notes
            on all_cats.pat_key = stg_pcoti_notes.pat_key
            and all_cats.visit_key = stg_pcoti_notes.visit_key
            and all_cats.init_note_date = stg_pcoti_notes.note_join_date
        left join {{ ref('stg_patient') }} as stg_patient
            on all_cats.pat_key = stg_patient.pat_key
        left join {{ ref('encounter_inpatient') }} as encounter_inpatient
            on all_cats.visit_key = encounter_inpatient.visit_key
        left join {{ ref('pcoti_cat_code_details_1' ) }} as pcoti_cat_code_details_1
            on all_cats.redcap_episode_event_key = pcoti_cat_code_details_1.episode_event_key
        left join {{ ref('pcoti_cat_code_details_3' ) }} as pcoti_cat_code_details_3
            on all_cats.redcap_episode_event_key = pcoti_cat_code_details_3.episode_event_key
        left join {{ ref('pcoti_cat_code_details_4' ) }} as pcoti_cat_code_details_4
            on all_cats.redcap_episode_event_key = pcoti_cat_code_details_4.episode_event_key
),

-- add details of first post-CAT ICU transfer
all_cats_w_xfer as (
    select
        inner_qry.*
    from (
        select
            all_cats_w_details.*,
            pcoti_icu_transfers.icu_enter_date,
            row_number() over(
                partition by all_cats_w_details.episode_key, all_cats_w_details.index_episode_event_key
                order by pcoti_icu_transfers.icu_enter_date
            ) as icu_xfer_seq
        from
            all_cats_w_details
            left join {{ ref('pcoti_icu_transfers') }} as pcoti_icu_transfers
                on all_cats_w_details.episode_key = pcoti_icu_transfers.episode_key
                and pcoti_icu_transfers.icu_enter_date >= all_cats_w_details.index_event_date
                and pcoti_icu_transfers.icu_enter_date <= all_cats_w_details.index_event_date + interval '24 hours'
                and pcoti_icu_transfers.icu_enter_date <= all_cats_w_details.index_event_date + interval '24 hours'
    ) as inner_qry
    where
        inner_qry.icu_xfer_seq = 1
)

select
    all_cats_w_xfer.episode_key,
    all_cats_w_xfer.pat_key,
    all_cats_w_xfer.visit_key,
    all_cats_w_xfer.index_episode_event_key,
    all_cats_w_xfer.index_source_event_type,
    all_cats_w_xfer.index_event_date,
    all_cats_w_xfer.mrn,
    all_cats_w_xfer.csn,
    all_cats_w_xfer.patient_name,
    all_cats_w_xfer.patient_dob,
    all_cats_w_xfer.redcap_episode_event_key,
    all_cats_w_xfer.redcap_record_id,
    all_cats_w_xfer.redcap_record_date,
    all_cats_w_xfer.ip_service_name,
    all_cats_w_xfer.dept_key,
    all_cats_w_xfer.department_name,
    all_cats_w_xfer.department_group_name,
    all_cats_w_xfer.campus_name,
    all_cats_w_xfer.dx_at_event,
    all_cats_w_xfer.cat_provider_type,
    all_cats_w_xfer.immediate_disposition,
    all_cats_w_xfer.survival_status,
    all_cats_w_xfer.init_note_episode_event_key,
    all_cats_w_xfer.init_note_date,
    all_cats_w_xfer.init_note_key,
    all_cats_w_xfer.init_note_id,
    all_cats_w_xfer.icu_enter_date,
    date_part(
        'epoch',
        all_cats_w_xfer.icu_enter_date - all_cats_w_xfer.index_event_date
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
    all_cats_w_xfer
