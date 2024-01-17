select
    coalesce(
        note_edit_metadata_history.visit_key,
        note_edit_metadata_history.note_visit_key)
    as visit_key,
    note_edit_metadata_history.mrn,
    coalesce(
        note_edit_metadata_history.provider_name,
        initcap(note_edit_metadata_history.final_author_name))
    as provider_name,
    note_edit_metadata_history.note_type,
    'note_non_registry' as data_source,
    coalesce(
        note_edit_metadata_history.service_date,
        note_text.contact_dt)
    as note_encounter_date
from
    {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
    left join  {{ source('cdw', 'note_text') }} as note_text
        on note_edit_metadata_history.note_visit_key = note_text.note_visit_key
where
    lower(note_text) like '%heart valve conference note%'
    and year(add_months(service_date, 6)) > '2020'
    and last_edit_ind = 1
group by
    note_edit_metadata_history.visit_key,
    note_edit_metadata_history.note_visit_key,
    note_edit_metadata_history.mrn,
    note_edit_metadata_history.provider_name,
    note_edit_metadata_history.final_author_name,
    note_edit_metadata_history.note_type,
    data_source,
    note_edit_metadata_history.service_date,
    note_text.contact_dt
