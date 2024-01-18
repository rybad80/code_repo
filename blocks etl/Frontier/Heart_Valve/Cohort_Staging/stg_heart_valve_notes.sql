with
registry_mrns as (
    select
        mrn
    from
        {{ ref('cardiac_valve_center') }}
    group by
        mrn
),
still_missing as (
    select
        registry_mrns.mrn
    from
        registry_mrns
        left join {{ ref('stg_heart_valve_notes_proc2') }} as combine
            on registry_mrns.mrn = combine.mrn
    group by
        registry_mrns.mrn,
        combine.data_source
    having combine.data_source is null
),
find_still_missing_note as (
    select
        coalesce(visit_key, cast(cardiac_valve_center.record_id as int)) as visit_key,
        temp_source.mrn,
        coalesce(provider_name, referring_provider) as provider_name,
        coalesce(note_type, 'sourced from registry') as note_type,
        'find_still_missing_note' as data_source,
        coalesce(service_date, date_of_referral) as note_encounter_date
    from
        {{ ref('cardiac_valve_center') }} as cardiac_valve_center
        inner join still_missing as temp_source
            on cardiac_valve_center.mrn = temp_source.mrn
        left join {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
            on temp_source.mrn = note_edit_metadata_history.mrn
        and (lower(note_edit_metadata_history.provider_name) in (
                        'baumgarten, sara e',
                        'coleman, keith',
                        'jolley, matthew',
                        'padiyath, asif',
                        'quartermain, michael',
                        'rogers, lindsay s',
                        'savla, jill'
                        )
                        or lower(note_edit_metadata_history.version_author_provider_name) in (
                            'baumgarten, sara e',
                            'coleman, keith',
                            'jolley, matthew',
                            'padiyath, asif',
                            'quartermain, michael',
                            'rogers, lindsay s',
                            'savla, jill'
                            )
                        )
                and last_edit_ind = 1
                and lower(specialty_name) = 'cardiology'
                and lower(note_type) in (
                                        'progress notes',
                                        'consults',
                                        'consult note'
                                        )
                and year(add_months(encounter_date, 6)) > '2020'
                and year(add_months(encounter_date, 6)) <= '2023'
            group by
                visit_key,
                record_id,
                temp_source.mrn,
                provider_name,
                referring_provider,
                note_type,
                service_date,
                date_of_referral
)
select
    visit_key,
    mrn,
    provider_name,
    note_type,
    data_source,
    note_encounter_date
from {{ ref('stg_heart_valve_notes_proc1') }}
union all
select
    visit_key,
    mrn,
    provider_name,
    note_type,
    data_source,
    note_encounter_date
from {{ ref('stg_heart_valve_notes_proc2') }}
union all
select
    visit_key,
    mrn,
    provider_name,
    note_type,
    data_source,
    note_encounter_date
from find_still_missing_note
