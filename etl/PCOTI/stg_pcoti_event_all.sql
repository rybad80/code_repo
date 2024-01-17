-- Add all ADT events
with event_location as (
    select
        stg_pcoti_event_adt_grouped.pat_key,
        stg_pcoti_event_adt_grouped.visit_key,
        stg_pcoti_event_adt_grouped.event_type_name,
        stg_pcoti_event_adt_grouped.event_type_abbrev,
        stg_pcoti_event_adt_grouped.dept_key,
        stg_pcoti_event_adt_grouped.department_name,
        stg_pcoti_event_adt_grouped.department_group_name,
        stg_pcoti_event_adt_grouped.bed_care_group,
        stg_pcoti_event_adt_grouped.campus_name,
        stg_pcoti_event_adt_grouped.event_start_date,
        stg_pcoti_event_adt_grouped.event_end_date
    from
         {{ ref('stg_pcoti_event_adt_grouped') }} as stg_pcoti_event_adt_grouped
),

-- Add all surgery and sugery-anesthesia events
event_surgery as (
    select
        stg_pcoti_event_surgery.pat_key,
        stg_pcoti_event_surgery.visit_key,
        stg_pcoti_event_surgery.event_type_name,
        stg_pcoti_event_surgery.event_type_abbrev,
        stg_pcoti_event_surgery.event_start_date,
        stg_pcoti_event_surgery.event_end_date
    from
        {{ ref('stg_pcoti_event_surgery') }} as stg_pcoti_event_surgery
),

-- Add all intubation events
event_intubation as (
    select
        stg_pcoti_event_intubation.pat_key,
        stg_pcoti_event_intubation.visit_key,
        stg_pcoti_event_intubation.event_type_name,
        stg_pcoti_event_intubation.event_type_abbrev,
        stg_pcoti_event_intubation.event_start_date,
        stg_pcoti_event_intubation.event_end_date
    from
        {{ ref('stg_pcoti_event_intubation') }} as stg_pcoti_event_intubation
),

-- Add all CAT/Code note events
event_note as (
    select
        stg_pcoti_event_note.pat_key,
        stg_pcoti_event_note.visit_key,
        stg_pcoti_event_note.event_type_name,
        stg_pcoti_event_note.event_type_abbrev,
        stg_pcoti_event_note.event_start_date,
        stg_pcoti_event_note.event_end_date
    from
        {{ ref('stg_pcoti_event_note') }} as stg_pcoti_event_note
),

-- Add all watcher FS events
event_watcher as (
    select
        stg_pcoti_event_watcher_activation.pat_key,
        stg_pcoti_event_watcher_activation.visit_key,
        stg_pcoti_event_watcher_activation.event_type_name,
        stg_pcoti_event_watcher_activation.event_type_abbrev,
        stg_pcoti_event_watcher_activation.event_start_date,
        stg_pcoti_event_watcher_activation.event_end_date
    from
        {{ ref('stg_pcoti_event_watcher_activation') }} as stg_pcoti_event_watcher_activation
),

-- Add all transfer review events
event_transfer_review as (
    select
        stg_pcoti_event_transfer_review.pat_key,
        stg_pcoti_event_transfer_review.visit_key,
        stg_pcoti_event_transfer_review.event_type_name,
        stg_pcoti_event_transfer_review.event_type_abbrev,
        stg_pcoti_event_transfer_review.event_start_date,
        stg_pcoti_event_transfer_review.event_end_date
    from
        {{ ref('stg_pcoti_event_transfer_review') }} as stg_pcoti_event_transfer_review
),

-- Add all vasopressor events
event_vasopressor as (
    select
        stg_pcoti_event_vasopressor.pat_key,
        stg_pcoti_event_vasopressor.visit_key,
        stg_pcoti_event_vasopressor.event_type_name,
        stg_pcoti_event_vasopressor.event_type_abbrev,
        stg_pcoti_event_vasopressor.event_start_date,
        stg_pcoti_event_vasopressor.event_end_date
    from
        {{ ref('stg_pcoti_event_vasopressor') }} as stg_pcoti_event_vasopressor
),

-- Add all fluid bolus >=60 events
event_fluid_bolus as (
    select
        stg_pcoti_event_bolus_gt60.pat_key,
        stg_pcoti_event_bolus_gt60.visit_key,
        stg_pcoti_event_bolus_gt60.event_type_name,
        stg_pcoti_event_bolus_gt60.event_type_abbrev,
        stg_pcoti_event_bolus_gt60.event_start_date,
        stg_pcoti_event_bolus_gt60.event_end_date
    from
        {{ ref('stg_pcoti_event_bolus_gt60') }} as stg_pcoti_event_bolus_gt60
),

-- Combine all non-ADT events in prep for join to get location info
event_non_adt as (
    select * from event_surgery
    union all
    select * from event_intubation
    union all
    select * from event_note
    union all
    select * from event_watcher
    union all
    select * from event_transfer_review
    union all
    select * from event_vasopressor
    union all
    select * from event_fluid_bolus
),

-- Create augmented version of event_non_adt that includes location info from
-- ADT events CTE
event_non_adt_augmented as (
    select
        event_non_adt.pat_key,
        event_non_adt.visit_key,
        event_non_adt.event_type_name,
        event_non_adt.event_type_abbrev,
        event_location.dept_key,
        event_location.department_name,
        event_location.department_group_name,
        event_location.bed_care_group,
        event_location.campus_name,
        event_non_adt.event_start_date,
        event_non_adt.event_end_date
    from
        event_non_adt
        left join event_location
            on event_non_adt.pat_key = event_location.pat_key
            and event_non_adt.visit_key = event_location.visit_key
            and event_non_adt.event_start_date >= event_location.event_start_date
            and event_non_adt.event_start_date <= event_location.event_end_date
),

-- Join together ADT location events and augmented version of all other events
event_union as (
    select * from event_location
    union all
    select * from event_non_adt_augmented
),

-- REDCap events have their locations determine based on field in survey
-- ICU transfer data has episode_event_key set separately
event_final as (
    select
        {{ dbt_utils.surrogate_key([
            'event_union.pat_key',
            'event_union.visit_key',
            'event_union.event_type_abbrev',
            'event_union.event_start_date'
        ]) }} as episode_event_key,
        {{ dbt_utils.surrogate_key([
            'event_union.pat_key',
            'event_union.visit_key'
        ]) }} as episode_key,
        event_union.pat_key,
        event_union.visit_key,
        null as redcap_record_id,
        event_union.event_type_name,
        event_union.event_type_abbrev,
        event_union.dept_key,
        event_union.department_name,
        event_union.department_group_name,
        event_union.bed_care_group,
        event_union.campus_name,
        event_union.event_start_date,
        event_union.event_end_date
    from
        event_union

    union all

    select
        {{ dbt_utils.surrogate_key([
            'stg_pcoti_event_redcap.pat_key',
            'stg_pcoti_event_redcap.record_id',
            'stg_pcoti_event_redcap.event_type_abbrev',
            'stg_pcoti_event_redcap.event_start_date'
        ]) }} as episode_event_key,
        {{ dbt_utils.surrogate_key([
            'stg_pcoti_event_redcap.pat_key',
            'stg_pcoti_event_redcap.visit_key_or_record_id'
        ]) }} as episode_key,
        stg_pcoti_event_redcap.pat_key,
        stg_pcoti_event_redcap.visit_key,
        stg_pcoti_event_redcap.record_id as redcap_record_id,
        stg_pcoti_event_redcap.event_type_name,
        stg_pcoti_event_redcap.event_type_abbrev,
        stg_pcoti_event_redcap.dept_key,
        stg_pcoti_event_redcap.department_name,
        stg_pcoti_event_redcap.department_group_name,
        stg_pcoti_event_redcap.bed_care_group,
        stg_pcoti_event_redcap.campus_name,
        stg_pcoti_event_redcap.event_start_date,
        stg_pcoti_event_redcap.event_end_date
    from
        {{ ref('stg_pcoti_event_redcap') }} as stg_pcoti_event_redcap

    union all

    select
        pcoti_icu_transfers.episode_event_key,
        pcoti_icu_transfers.episode_key,
        pcoti_icu_transfers.pat_key,
        pcoti_icu_transfers.visit_key,
        null as redcap_record_id,
        pcoti_icu_transfers.icu_xfer_event_type_name as event_type_name,
        pcoti_icu_transfers.icu_xfer_event_type_abbrev as event_type_abbrev,
        pcoti_icu_transfers.from_dept_key as dept_key,
        pcoti_icu_transfers.from_department_name as department_name,
        pcoti_icu_transfers.from_department_group_name as department_group_name,
        pcoti_icu_transfers.from_bed_care_group as bed_care_group,
        pcoti_icu_transfers.from_campus_name as campus_name,
        pcoti_icu_transfers.icu_enter_date as event_start_date,
        null as event_end_date
    from
        {{ ref('pcoti_icu_transfers') }} as pcoti_icu_transfers
),

first_ip_service as (
    select
        event_final.episode_event_key,
        max(
            case
                when adt_service.service = 'NOT APPLICABLE' then null
                else adt_service.service
            end
        ) as ip_service_name
    from
        event_final
        left join {{ ref('adt_service')}} as adt_service
            on event_final.pat_key = adt_service.pat_key
            and event_final.visit_key = adt_service.visit_key
            and event_final.event_start_date >= adt_service.service_start_datetime
            and event_final.event_start_date <= adt_service.service_end_datetime
    group by
        event_final.episode_event_key
)

-- Get all events
select
    event_final.*,
    first_ip_service.ip_service_name
from
    event_final
    left join first_ip_service
        on event_final.episode_event_key = first_ip_service.episode_event_key
