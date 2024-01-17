{{ config(meta = {
    'critical': true
}) }}

with adt_dept_w_team as (
    select
        stg_adt_all.visit_key,
        stg_adt_all.visit_event_key,
        case
            when stg_adt_all.inpatient_department_order = 1
            then 1 else 0
        end as admission_team_event_ind,
        case
            when stg_adt_all.currently_admitted_ind = 0
                and stg_adt_all.last_department_ind = 1
            then 1 else 0
        end as discharge_team_event_ind,
        stg_adt_treatment_team.start_team_name,
        stg_adt_treatment_team.end_team_name
    from
        {{ ref('stg_adt_all') }} as stg_adt_all
        inner join {{ ref('stg_adt_treatment_team') }} as stg_adt_treatment_team
            on stg_adt_treatment_team.visit_event_key = stg_adt_all.visit_event_key
    /*only need ip encounters*/
    where
        stg_adt_all.department_ind = 1
        and stg_adt_all.ip_enter_date is not null
        and stg_adt_all.hospital_discharge_date >= '2021-04-01'
),

encounters as (
    select distinct
        visit_key
    from
        adt_dept_w_team
)

select
    encounters.visit_key,
    admission_team.start_team_name as admission_team,
    discharge_team.end_team_name as discharge_team
from
    encounters
    left join adt_dept_w_team as admission_team
        on admission_team.visit_key = encounters.visit_key
        and admission_team.admission_team_event_ind = 1
    left join adt_dept_w_team as discharge_team
        on discharge_team.visit_key = encounters.visit_key
        and discharge_team.discharge_team_event_ind = 1
