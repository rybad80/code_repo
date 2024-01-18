{{ config(meta = {
    'critical': true
}) }}

with treatment_team_stage as (
    select distinct
        visit_key,
        provider_care_team_start_date,
        provider_care_team_end_date,
        /*If teams have identical start and end dates replace name with 'Multiple Teams'
        to be converted to NULL in final select*/
        case
            when count(*) over (
                partition by
                    visit_key,
                    provider_care_team_start_date,
                    provider_care_team_end_date
                ) > 1
                then 'Multiple Teams'
            /*Otherwise use actual name*/
            else upper(coalesce(provider_care_team_group_name, provider_care_team_name))
        end as team_name
    from
        {{ref('stg_provider_encounter_care_team')}}
    where
        /* these services are usually paired with a floc team
         we care more about the floc teams as these are just "services" */
        lower(provider_care_team_name) != 'cardiology'
        and not regexp_like(
            provider_care_team_name,
            -- ex. matches 'oncology bmt' and 'oncology, solid'
            'oncology,? (bmt|liquid|neuro|solid)',
            1,
            'i' --case insensitive
        )
),

adt_department_stage as (
    select
        visit_event_key,
        visit_key,
        pat_key,
        dept_enter_date as enter_date,
        dept_exit_date as exit_date,
        dept_exit_date_or_current_date as exit_date_or_current_date
    from
        {{ ref('stg_adt_all') }}
    where
        department_ind = 1
),

/*Treatment team for an ADT event is:
1. Most recently added team active at start of event
2. If no active teams, the first team added after start of event*/

/*Most recent active team at start of adt event*/
active_team_at_start as (
    select
        adt_department_stage.visit_event_key,
        treatment_team_stage.team_name,
        row_number() over (
            partition by
                adt_department_stage.visit_event_key
            order by
                treatment_team_stage.provider_care_team_start_date desc
        ) as treatment_team_order_desc
    from
        adt_department_stage
        /* _active_ treatment team on episode start */
        left join treatment_team_stage
            on treatment_team_stage.visit_key = adt_department_stage.visit_key
                and treatment_team_stage.provider_care_team_start_date <= adt_department_stage.enter_date
                and coalesce(treatment_team_stage.provider_care_team_end_date, current_timestamp)
                >= adt_department_stage.enter_date
),

/*First team added at start of adt event*/
first_team_after_start as (
    select
        adt_department_stage.visit_event_key,
        treatment_team_stage.team_name,
        row_number() over (
            partition by
                adt_department_stage.visit_event_key
            order by
                treatment_team_stage.provider_care_team_start_date
        ) as treatment_team_order_asc

    from
        adt_department_stage
        inner join treatment_team_stage
            on treatment_team_stage.visit_key = adt_department_stage.visit_key
                and treatment_team_stage.provider_care_team_start_date
                between adt_department_stage.enter_date and adt_department_stage.exit_date_or_current_date
),

/*Last team ending before end of adt event*/
/*Placeholder logic that at least allows us to get the hopsital discharge team*/
last_team_before_end as (
    select
        adt_department_stage.visit_event_key,
        treatment_team_stage.team_name,
        row_number() over (
            partition by
                adt_department_stage.visit_event_key
            order by
                treatment_team_stage.provider_care_team_end_date desc,
                treatment_team_stage.provider_care_team_start_date desc
        ) as treatment_team_order_desc

    from
        adt_department_stage
        inner join treatment_team_stage
            on treatment_team_stage.visit_key = adt_department_stage.visit_key
                and treatment_team_stage.provider_care_team_end_date
                <= adt_department_stage.exit_date
)

select
    adt_department_stage.visit_event_key,
    adt_department_stage.visit_key,
    adt_department_stage.pat_key,
    case when
        coalesce(active_team_at_start.team_name, first_team_after_start.team_name) != 'Multiple Teams'
        then coalesce(active_team_at_start.team_name, first_team_after_start.team_name)
    end as start_team_name,
    case
        when last_team_before_end.team_name != 'Multiple Teams'
        then last_team_before_end.team_name
    end as end_team_name
from
    adt_department_stage
    left join active_team_at_start
        on active_team_at_start.visit_event_key = adt_department_stage.visit_event_key
            and active_team_at_start.treatment_team_order_desc = 1
    left join first_team_after_start
        on first_team_after_start.visit_event_key = adt_department_stage.visit_event_key
            and first_team_after_start.treatment_team_order_asc = 1
    left join last_team_before_end
        on last_team_before_end.visit_event_key = adt_department_stage.visit_event_key
            and last_team_before_end.treatment_team_order_desc = 1
