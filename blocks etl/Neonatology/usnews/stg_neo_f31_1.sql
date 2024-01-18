/*
Percentage of patient first postoperative temperatures (done within 60 min)
that were < 36°C (<96.8°F) in the past yr?
*/

with fake_fact_periop_timestamps as (
    /* fact_periop_timestamps does not exist in blocks / stacks.
    Temporarily recreating the logic here to capture the out_room timestamp. */
    select
        or_log_case_times.log_key,
        or_log.admit_visit_key as visit_key,
        min(
            case
                when cdw_dictionary.src_id = 10
                then or_log_case_times.event_in_dt
            end
        ) as out_room

    from
        {{ source('cdw', 'or_log_case_times') }} as or_log_case_times
        inner join {{ source('cdw', 'or_log') }} as or_log
            on or_log.log_key = or_log_case_times.log_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as cdw_dictionary
            on cdw_dictionary.dict_key = or_log_case_times.dict_or_pat_event_key

    group by
        or_log_case_times.log_key,
        or_log.admit_visit_key

    having
        out_room is not null
),

all_temps as (
    select
        flowsheet_vitals.mrn,
        flowsheet_vitals.patient_name,
        flowsheet_vitals.dob,
        flowsheet_vitals.visit_key,
        fake_fact_periop_timestamps.log_key,
        flowsheet_vitals.temperature_c as temperature,
        flowsheet_vitals.recorded_date as temperature_timestamp,
        extract( --noqa: PRS
            epoch from flowsheet_vitals.recorded_date - fake_fact_periop_timestamps.out_room
        ) / 3600.0 as time_to_temp_in_hours,
        row_number() over (
            partition by
                fake_fact_periop_timestamps.visit_key,
                fake_fact_periop_timestamps.out_room
            order by
                flowsheet_vitals.recorded_date asc
        ) as rn

    from
        fake_fact_periop_timestamps
        inner join {{ ref('neo_nicu_episode') }} as neo_nicu_episode
            on neo_nicu_episode.visit_key = fake_fact_periop_timestamps.visit_key
            and date(fake_fact_periop_timestamps.out_room) >= date(neo_nicu_episode.episode_start_date)
            and date(fake_fact_periop_timestamps.out_room) <= coalesce(
                date(neo_nicu_episode.episode_start_date),
                current_date
            )
        inner join {{ ref('flowsheet_vitals') }} as flowsheet_vitals
            on flowsheet_vitals.visit_key = fake_fact_periop_timestamps.visit_key

    where
        time_to_temp_in_hours between 0.0 and 1.0
        and flowsheet_vitals.temperature_c is not null
)

select
    'clinical' as domain, --noqa: L029
    null as subdomain,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    all_temps.mrn,
    all_temps.patient_name,
    all_temps.dob,
    all_temps.temperature_timestamp as index_date,
    all_temps.log_key as primary_key,
    case
        /* 36.0 C = 96.8F */
        when temperature < 96.8 then 1
        else 0
    end as num,
    1 as denom
from
    all_temps
    inner join {{ ref('usnews_metadata_calendar') }} as usnews_metadata_calendar
        on date(all_temps.temperature_timestamp)
            between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date

where
    all_temps.rn = 1
    and usnews_metadata_calendar.question_number = 'f31.1'
