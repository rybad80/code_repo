with visits_with_violent_restraints as (
    --region gather unique visit keys
    select
        visit_key
    from
        {{ ref('stg_restraints')}}
    where violent_restraint_ind = 1
    group by
        visit_key
)

--identify .RESTRAINT components in notes
--"run" with following sequence to prevent note separation
select
    visits_with_violent_restraints.visit_key,
    note_edit_metadata_history.note_key,
    --combine current and following note in case phrase runs to next page
    lower(
        note_text.note_text
        || coalesce(
            lead(note_text.note_text) over(
                partition by
                    note_text.note_id
                order by note_text.line
            ),
            ''
        )
    ) as note_concat,
    note_text.line as seq_num,
    note_edit_metadata_history.service_date
from
    visits_with_violent_restraints
    inner join {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
        on visits_with_violent_restraints.visit_key = note_edit_metadata_history.visit_key
    inner join {{ source('clarity_ods', 'hno_note_text') }} as note_text
        on note_edit_metadata_history.note_enc_id = note_text.note_csn_id
where
    --use most up-to-date note
    note_edit_metadata_history.last_edit_ind = 1
    and note_edit_metadata_history.note_type_id in (
        '1', --Inpatient Progress Notes
        '19' --ED Provider Notes
    )
    and note_edit_metadata_history.note_deleted_ind = 0
    and note_edit_metadata_history.note_key != -1
