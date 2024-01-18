with
cool_start as (
    select distinct
        visit_ed_event.visit_key,
        visit_ed_event.event_dt,
        master_event_type.event_desc,
        master_event_type.event_id,
        row_number() over (
            partition by visit_ed_event.visit_key,
            master_event_type.event_id
            order by
                visit_ed_event.event_dt
        ) as rowno
    from
        {{source('cdw', 'visit_ed_event')}} as visit_ed_event
        inner join {{source('cdw', 'master_event_type')}} as master_event_type
          on visit_ed_event.event_type_key = master_event_type.event_type_key
    where
        master_event_type.event_id in (112700005)
        and event_stat is null
),
first_cool_start as (
    select distinct
        visit_key,
        event_dt
    from
        cool_start
    where
        rowno = 1
),
cool as (
    select distinct
        visit_ed_event.visit_key,
        visit_ed_event.event_dt,
        master_event_type.event_desc,
        master_event_type.event_id,
        row_number() over (
            partition by visit_ed_event.visit_key,
            master_event_type.event_id
            order by
                visit_ed_event.event_dt
        ) as rowno
    from
        first_cool_start
        inner join {{source('cdw', 'visit_ed_event')}} as visit_ed_event
          on first_cool_start.visit_key = visit_ed_event.visit_key
        inner join {{source('cdw', 'master_event_type')}} as master_event_type
          on visit_ed_event.event_type_key = master_event_type.event_type_key
    where
        master_event_type.event_id in (112700005, 112700006)
        and event_stat is null
),
cool_times as (
    select
        visit_key,
        max(
            case
                when event_id = 112700005
                and rowno = 1 then event_dt
            end
        ) as cool_start_date_1,
        max(
            case
                when event_id = 112700006
                and rowno = 1 then event_dt
            end
        ) as cool_stop_date_1,
        max(
            case
                when event_id = 112700005
                and rowno = 2 then event_dt
            end
        ) as cool_start_date_2,
        max(
            case
                when event_id = 112700006
                and rowno = 2 then event_dt
            end
        ) as cool_stop_date_2,
        max(
            case
                when event_id = 112700005
                and rowno = 3 then event_dt
            end
        ) as cool_start_date_3,
        max(
            case
                when event_id = 112700006
                and rowno = 3 then event_dt
            end
        ) as cool_stop_date_3,
        max(
            case
                when event_id = 112700005
                and rowno = 4 then event_dt
            end
        ) as cool_start_date_4,
        max(
            case
                when event_id = 112700006
                and rowno = 4 then event_dt
            end
        ) as cool_stop_date_4,
        max(
            case
                when event_id = 112700005
                and rowno = 5 then event_dt
            end
        ) as cool_start_date_5,
        max(
            case
                when event_id = 112700006
                and rowno = 5 then event_dt
            end
        ) as cool_stop_date_5,
        min(
            case
                when event_id = 112700005 then event_dt
            end
        ) as first_cool_start_date,
        max(
            case
                when event_id = 112700006 then event_dt
            end
        ) as last_cool_stop_date
    from
        cool
    group by
        visit_key
),
final_times as (
    select
        encounter_all.visit_key,
        patient_all.mrn,
        patient_all.patient_name,
        patient_all.sex,
        patient_all.dob,
        encounter_all.csn,
        first_cool_start_date,
        last_cool_stop_date,
        cool_start_date_1,
        cool_stop_date_1,
        --noqa: prs
        case
            when cool_stop_date_2 is null then null
            else cool_start_date_2 --noqa: prs
        end as cool_start_date_2,
        case
            when cool_start_date_2 is null then null
            else cool_stop_date_2 --noqa: prs
        end as cool_stop_date_2,
        --noqa: prs --noqa: prs
        case
            when cool_stop_date_3 is null then null
            else cool_start_date_3 --noqa: prs
        end as cool_start_date_3,
        case
            when cool_start_date_3 is null then null
            else cool_stop_date_3 --noqa: prs
        end as cool_stop_date_3,
        --noqa: prs --noqa: prs
        case
            when cool_stop_date_4 is null then null
            else cool_start_date_4 --noqa: prs
        end as cool_start_date_4,
        case
            when cool_start_date_4 is null then null
            else cool_stop_date_4 --noqa: prs
        end as cool_stop_date_4,
        --noqa: prs --noqa: prs
        case
            when cool_stop_date_5 is null then null
            else cool_start_date_5 --noqa: prs
        end as cool_start_date_5,
        case
            when cool_start_date_5 is null then null
            else cool_stop_date_5 --noqa: prs
        end as cool_stop_date_5,
        --noqa: prs
        coalesce(
            extract(
                epoch
                from
                    cool_stop_date_1 - cool_start_date_1
            ) / 60,
            0
        ) + coalesce(
            extract(
                epoch
                from
                    cool_stop_date_2 - cool_start_date_2
            ) / 60,
            0
        ) + coalesce(
            extract(
                epoch
                from
                    cool_stop_date_3 - cool_start_date_3
            ) / 60,
            0
        ) + coalesce(
            extract(
                epoch
                from
                    cool_stop_date_4 - cool_start_date_4
            ) / 60,
            0
        ) + coalesce(
            extract(
                epoch
                from
                    cool_stop_date_5 - cool_start_date_5
            ) / 60,
            0
        ) as total_cool_minutes
    from
        cool_times
        inner join {{ref('encounter_all')}} as encounter_all on encounter_all.visit_key = cool_times.visit_key
        inner join {{ref('patient_all')}} as patient_all on patient_all.pat_key = encounter_all.pat_key
)
    select
        visit_key,
        mrn,
        patient_name,
        sex,
        dob,
        csn,
        first_cool_start_date,
        last_cool_stop_date,
        --noqa: prs
        cool_start_date_1,
        cool_stop_date_1,
        --noqa: prs
        cool_start_date_2,
        cool_stop_date_2,
        --noqa: prs
        cool_start_date_3,
        cool_stop_date_3,
        --noqa: prs
        cool_start_date_4,
        cool_stop_date_4,
        --noqa: prs
        cool_start_date_5,
        cool_stop_date_5,
        case
            when total_cool_minutes < 0 then extract(
                epoch
                from
                    last_cool_stop_date - first_cool_start_date
            ) / 60
            else total_cool_minutes
        end as total_cool_minutes
    from
        final_times
