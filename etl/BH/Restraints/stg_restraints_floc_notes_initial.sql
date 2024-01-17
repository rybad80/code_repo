with restraint_details as (
    --region details about each restraint
    --manual holds followed by device restraints should be linked but not interfere with one another
    select
        restraint_episode_key,
        epsd_start_key,
        visit_key,
        device_type, --manual hold or device restraint?
        restraint_start,
        restraint_removal,
        --identify next restraint with the same device type
        lead(restraint_start) over (
            partition by
                visit_key,
                device_type
            order by
                restraint_start
        ) as next_restraint_start
    from
        {{ ref('stg_restraints') }}
    where violent_restraint_ind = 1
),

notes_per_restraint_type as (
    --region left limit possible face-to-face evaluations by the restraint initaition time
    --multiple restraints can share a face-to-face evaluation and should therefore not interfere with one another 
    --only occurs when a manual restraint is followed by a device restraint
    select
        restraint_details.epsd_start_key,
        restraint_details.device_type,
        stg_restraints_floc_notes.note_key,
        stg_restraints_floc_notes.floc_datetime,
        case when minutes_between(stg_restraints_floc_notes.floc_datetime, restraint_start) < 60
            then 1 else 0 end as face_to_face_within_hr_ind,
        stg_restraints_floc_notes.floc_reasons_ind,
        stg_restraints_floc_notes.floc_pat_response_ind,
        stg_restraints_floc_notes.floc_attending_notified_ind,
        stg_restraints_floc_notes.floc_summary_ind,
        stg_restraints_floc_notes.note_complete_ind,
        --ignore notes submitted after a following restraint
        --not performed in where statement to prevent interference between linked restraints
        --if there is no following restraint of the same type, this defaults to 0
        case when floc_datetime >= restraint_details.next_restraint_start
            then 1 else 0 end as exclude_note_ind,
        --assign the exclusion indicator across restraint types
        --if a note is excluded for a device restraint, it should also be excluded for the linked manual hold
        max(exclude_note_ind) over (
            partition by
                note_key,
                floc_datetime --ED notes with multiple templates
            order by
                restraint_start,
                device_type
            rows between current row and unbounded following) as remove_ind
    from
        restraint_details
        inner join {{ ref('stg_restraints_floc_notes') }} as stg_restraints_floc_notes
            on restraint_details.visit_key = stg_restraints_floc_notes.visit_key
    where
        stg_restraints_floc_notes.floc_datetime >= restraint_details.restraint_start
    group by
        restraint_details.epsd_start_key,
        restraint_details.restraint_start,
        restraint_details.device_type,
        restraint_details.next_restraint_start,
        stg_restraints_floc_notes.note_key,
        stg_restraints_floc_notes.floc_datetime,
        stg_restraints_floc_notes.floc_reasons_ind,
        stg_restraints_floc_notes.floc_pat_response_ind,
        stg_restraints_floc_notes.floc_attending_notified_ind,
        stg_restraints_floc_notes.floc_summary_ind,
        stg_restraints_floc_notes.note_complete_ind
),

notes_per_restraint as (
    --region right-limit the notes across device types
    select
        epsd_start_key,
        floc_datetime,
        face_to_face_within_hr_ind,
        floc_reasons_ind,
        floc_pat_response_ind,
        floc_attending_notified_ind,
        floc_summary_ind,
        note_complete_ind,
        max(remove_ind) as removal_ind,
        rank() over(
            partition by
                epsd_start_key
            order by
                floc_datetime
        ) as note_number
    from
        notes_per_restraint_type
    group by
        epsd_start_key,
        floc_datetime,
        face_to_face_within_hr_ind,
        floc_reasons_ind,
        floc_pat_response_ind,
        floc_attending_notified_ind,
        floc_summary_ind,
        note_complete_ind
    having
        removal_ind = 0
)

--select the earliest record that satisfies the conditions
select
    stg_restraints.restraint_episode_key,
    notes_per_restraint.floc_datetime as first_eval_time,
    notes_per_restraint.face_to_face_within_hr_ind,
    notes_per_restraint.floc_reasons_ind,
    notes_per_restraint.floc_pat_response_ind,
    notes_per_restraint.floc_attending_notified_ind,
    notes_per_restraint.floc_summary_ind,
    notes_per_restraint.note_complete_ind
from
    {{ ref('stg_restraints') }} as stg_restraints
    inner join notes_per_restraint
        on stg_restraints.epsd_start_key = notes_per_restraint.epsd_start_key
where
    notes_per_restraint.note_number = 1
