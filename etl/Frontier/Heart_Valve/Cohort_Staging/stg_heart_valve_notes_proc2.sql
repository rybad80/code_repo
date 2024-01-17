with
registry_mrns as (
    select
        mrn
    from
        {{ ref('cardiac_valve_center') }}
    group by
        mrn
),
find_missing_note as (
    select
        visit_key,
        temp_source.mrn,
        provider_name,
        note_type,
        'find_missing_note' as data_source,
        service_date as note_encounter_date
    from
        registry_mrns as temp_source
        inner join {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
            on temp_source.mrn = note_edit_metadata_history.mrn
        left join {{ source('cdw', 'note_text') }} as note_text
            on note_edit_metadata_history.note_visit_key = note_text.note_visit_key
    where
        (lower(note_edit_metadata_history.provider_name) in (
                    'baumgarten, sara e',
                    'coleman, keith',
                    'jolley, matthew',
                    'padiyath, asif',
                    'quartermain, michael',
                    'rogers, lindsay s',
                    'savla, jill'
                    )
                    and lower(note_edit_metadata_history.version_author_provider_name) in (
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
                and year(add_months(encounter_date, 6)) <= '2024'
        group by
            visit_key,
            temp_source.mrn,
            provider_name,
        note_type,
            service_date
),
note_registry_no_mrn as (
    select
        record_id as visit_key,
        mrn,
        referring_provider as provider_name,
        'no MRN' as note_type,
        'note_registry_no_mrn' as data_source,
        date_of_referral as note_encounter_date
    from
        {{ ref('cardiac_valve_center') }}
    where
        mrn is null
    group by
        visit_key,
        mrn,
        provider_name,
        note_type,
        data_source,
        note_encounter_date
)
select
    visit_key,
    mrn,
    provider_name,
    note_type,
    data_source,
    note_encounter_date
from note_registry_no_mrn
union
select
    visit_key,
    mrn,
    provider_name,
    note_type,
    data_source,
    note_encounter_date
from find_missing_note
