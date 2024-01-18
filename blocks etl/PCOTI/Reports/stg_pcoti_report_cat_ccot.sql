with all_ccot_notes as (
    select
        pcoti_episode_events.*
    from
        {{ ref("pcoti_episode_events") }} as pcoti_episode_events
    where
        pcoti_episode_events.event_type_abbrev in (
            'NOTE_CCOT',
            'NOTE_CCOT_RN_RT'
        )
),

cats_with_repeats as (
    select
        stg_pcoti_report_catcode_all.episode_key,
        stg_pcoti_report_catcode_all.pat_key,
        stg_pcoti_report_catcode_all.visit_key,
        stg_pcoti_report_catcode_all.episode_event_key,
        stg_pcoti_report_catcode_all.event_type,
        stg_pcoti_report_catcode_all.event_start_date,
        stg_pcoti_report_catcode_all.mrn,
        stg_pcoti_report_catcode_all.csn,
        stg_pcoti_report_catcode_all.patient_name,
        stg_pcoti_report_catcode_all.patient_dob,
        stg_pcoti_report_catcode_all.ip_service_name,
        stg_pcoti_report_catcode_all.department_name,
        stg_pcoti_report_catcode_all.department_group_name,
        stg_pcoti_report_catcode_all.campus_name,
        stg_pcoti_report_catcode_all.icu_enter_date,
        stg_pcoti_report_catcode_all.immediate_disposition,
        stg_pcoti_report_catcode_all.init_note_episode_event_key,
        lead(stg_pcoti_report_catcode_all.event_start_date, 1) over (
            partition by stg_pcoti_report_catcode_all.episode_key
            order by stg_pcoti_report_catcode_all.event_start_date
        ) as next_catcode_in_episode_date
    from
        {{ ref("stg_pcoti_report_catcode_all") }} as stg_pcoti_report_catcode_all
),

cats_for_metric as (
    select
        *,
        case
            when (
                next_catcode_in_episode_date is null
                or extract(
                    epoch
                    from next_catcode_in_episode_date
                    - event_start_date
                ) > 6
            )
            and (
                extract(dow from event_start_date) in (2, 3, 4, 5)
                or (
                    extract(dow from event_start_date) = 1
                    and hour(event_start_date) >= 19
                )
            )
            and event_type = 'CAT Call'
            and immediate_disposition = 'remained in initial location'
            and init_note_episode_event_key is not null
            then 1
            else 0
        end as include_cat_ind
    from
        cats_with_repeats
),

notes_cats_joined as (
    select
        cats_for_metric.episode_key,
        cats_for_metric.pat_key,
        cats_for_metric.visit_key,
        cats_for_metric.episode_event_key,
        cats_for_metric.event_start_date,
        cats_for_metric.mrn,
        cats_for_metric.csn,
        cats_for_metric.patient_name,
        cats_for_metric.patient_dob,
        cats_for_metric.ip_service_name,
        cats_for_metric.department_name,
        cats_for_metric.department_group_name,
        cats_for_metric.campus_name,
        all_ccot_notes.event_type_abbrev as ccot_note_type,
        all_ccot_notes.event_start_date as ccot_note_date
    from
        cats_for_metric
        left join all_ccot_notes
            on cats_for_metric.episode_key = all_ccot_notes.episode_key
            and all_ccot_notes.event_start_date
                >= cats_for_metric.event_start_date - interval '10 minutes'
            and all_ccot_notes.event_start_date
                <= cats_for_metric.event_start_date + interval '36 hours'
            and (
                cats_for_metric.icu_enter_date is null
                or cats_for_metric.icu_enter_date > cats_for_metric.event_start_date
                    + interval '36 hours'
            )
    where
        cats_for_metric.include_cat_ind = 1
)

select
    notes_cats_joined.episode_key,
    notes_cats_joined.pat_key,
    notes_cats_joined.visit_key,
    notes_cats_joined.episode_event_key,
    notes_cats_joined.event_start_date,
    notes_cats_joined.mrn,
    notes_cats_joined.csn,
    notes_cats_joined.patient_name,
    notes_cats_joined.patient_dob,
    notes_cats_joined.ip_service_name,
    notes_cats_joined.department_name,
    notes_cats_joined.department_group_name,
    notes_cats_joined.campus_name,
    max(
        case
            when notes_cats_joined.ccot_note_type = 'NOTE_CCOT' then 1
            else 0
        end
    ) as ccot_cat_followup_36hrs_ind,
    min(
        case
            when notes_cats_joined.ccot_note_type = 'NOTE_CCOT' then notes_cats_joined.ccot_note_date
            else null
        end
    ) as ccot_cat_followup_36hrs_date,
    max(
        case
            when notes_cats_joined.ccot_note_type = 'NOTE_CCOT_RN_RT' then 1
            else 0
        end
    ) as ccot_rnrt_cat_followup_36hrs_ind,
    min(
        case
            when notes_cats_joined.ccot_note_type = 'NOTE_CCOT_RN_RT' then notes_cats_joined.ccot_note_date
            else null
        end
    ) as ccot_rnrt_cat_followup_36hrs_date
from
    notes_cats_joined
group by
    notes_cats_joined.episode_key,
    notes_cats_joined.pat_key,
    notes_cats_joined.visit_key,
    notes_cats_joined.episode_event_key,
    notes_cats_joined.event_start_date,
    notes_cats_joined.mrn,
    notes_cats_joined.csn,
    notes_cats_joined.patient_name,
    notes_cats_joined.patient_dob,
    notes_cats_joined.ip_service_name,
    notes_cats_joined.department_name,
    notes_cats_joined.department_group_name,
    notes_cats_joined.campus_name
