with evaluations_cleaned as (
    --region convert time from string to date
    select
        *,
        --remove spaces and semicolon from timestamp
        regexp_replace(floc_time, ' |:', '') as floc_timestamp_raw,
        regexp_replace(floc_timestamp_raw, '(am|pm)', '') as floc_timestamp,
        coalesce(
            regexp_extract(floc_time, 'am|pm'),
            ''
        ) as am_or_pm,
        /*Hour*/
        case when floc_timestamp = '' --Occurs when a date is given for the face-to-face, but no time
            then null
            when length(floc_timestamp) in (1, 2) --Beginning of the hour- number between 00 and 23
            then floc_timestamp
            else substring(floc_timestamp, 1, length(floc_timestamp) - 2)
            end as floc_hour_raw,
        case when floc_hour_raw > 12 and am_or_pm = 'pm'
            then cast(floc_hour_raw as integer) - 12
            else cast(floc_hour_raw as integer) end as floc_hour,
        /*Minute*/
        case when length(floc_timestamp) in (1, 2)
            then '00'
            else substring(floc_timestamp, length(floc_timestamp) - 1, 2)
            end as floc_minute,
        --ensure the time provided is not incorrect
        case when floc_hour > 23
            then null
            else time(
                floc_hour
                || ':'
                || floc_minute
                || case when floc_hour > 12 then '' else am_or_pm end
            ) end as floc_time_processed,
        --create full timestamp`
        coalesce(
            floc_date,
            date(service_date)
            )
            + coalesce(
                floc_time_processed,
                time(service_date)
            ) as floc_datetime
    from {{ ref('stg_restraints_floc_evaluations') }}
),

evaluations_per_note as (
    --"run" the note sequences to create complete .RESTRAINT templates
    select
        visit_key,
        note_key,
        floc_datetime,
        group_id,
        seq_num as template_start,
        evaluation_index as template_start_index,
        lead(seq_num) over(
            partition by
                note_key
            order by
                seq_num
        ) as next_template_start,
        lead(evaluation_index) over(
            partition by
                note_key
            order by
                evaluation_index
        ) as next_template_start_index
    from
        evaluations_cleaned
    where
        floc_time_processed is not null
)

--region 
select
    evaluations_per_note.visit_key,
    evaluations_per_note.note_key,
    evaluations_per_note.floc_datetime,
    --if the group index exists, use it. Else, assume violent restraint
    max(case when evaluations_cleaned.group_index < evaluations_per_note.template_start_index
        then evaluations_per_note.group_id else 40071755 end) as restraint_id,
    --validate the text is for the same template
    max(case when evaluations_cleaned.reasons_index between
        evaluations_per_note.template_start_index
        and coalesce(evaluations_per_note.next_template_start_index, evaluations_cleaned.reasons_index)
        then evaluations_cleaned.reasons_ind else 0 end) as floc_reasons_ind,
    max(case when evaluations_cleaned.pat_response_index between
        evaluations_per_note.template_start_index
        and coalesce(evaluations_per_note.next_template_start_index, evaluations_cleaned.pat_response_index)
        then evaluations_cleaned.pat_response_ind else 0 end) as floc_pat_response_ind,
    max(case when evaluations_cleaned.attending_notified_index between
        evaluations_per_note.template_start_index
        and coalesce(evaluations_per_note.next_template_start_index, evaluations_cleaned.attending_notified_index)
        then evaluations_cleaned.attending_notified_ind else 0 end) as floc_attending_notified_ind,
    max(case when evaluations_cleaned.summary_index between
        evaluations_per_note.template_start_index
        and coalesce(evaluations_per_note.next_template_start_index, evaluations_cleaned.summary_index)
        then evaluations_cleaned.summary_ind else 0 end) as floc_summary_ind,
    (floc_reasons_ind
        + floc_pat_response_ind
        + floc_attending_notified_ind
        + floc_summary_ind) / 4 as note_complete_ind
from
    evaluations_per_note
    left join evaluations_cleaned
        on evaluations_per_note.note_key = evaluations_cleaned.note_key
        and evaluations_cleaned.seq_num >= evaluations_per_note.template_start
        and (
            evaluations_cleaned.seq_num <= evaluations_per_note.next_template_start
            or evaluations_per_note.next_template_start is null
        )
group by
    evaluations_per_note.visit_key,
    evaluations_per_note.note_key,
    evaluations_per_note.floc_datetime
having
    --exclude evaluations for non-violent restraints
    restraint_id = 40071755 --Violent Restraint
