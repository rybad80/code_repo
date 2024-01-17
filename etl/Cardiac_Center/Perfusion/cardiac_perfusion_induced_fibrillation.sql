with induced_fibrillation_start as (
    select distinct
        visit_ed_event.visit_key,
        visit_ed_event.event_dt,
        master_event_type.event_desc,
        master_event_type.event_id,
        row_number() over (
            partition by visit_ed_event.visit_key, master_event_type.event_id
            order by visit_ed_event.event_dt
        ) as rowno
    from
        {{source('cdw', 'visit_ed_event')}} as visit_ed_event
        inner join {{source('cdw', 'master_event_type')}} as master_event_type
            on visit_ed_event.event_type_key = master_event_type.event_type_key
    where
        master_event_type.event_id in (112700009)
        and event_stat is null
),
first_induced_fibrillation_start as (
    select distinct
        visit_key,
        event_dt
    from
        induced_fibrillation_start
    where
        rowno = 1
),
induced_fibrillation as (
    select distinct
        visit_ed_event.visit_key,
        visit_ed_event.event_dt,
        master_event_type.event_desc,
        master_event_type.event_id,
        row_number() over (
            partition by visit_ed_event.visit_key, master_event_type.event_id
            order by visit_ed_event.event_dt
        ) as rowno
    from
        first_induced_fibrillation_start
        inner join {{source('cdw', 'visit_ed_event')}} as visit_ed_event
            on first_induced_fibrillation_start.visit_key = visit_ed_event.visit_key
        inner join {{source('cdw', 'master_event_type')}} as master_event_type
            on visit_ed_event.event_type_key = master_event_type.event_type_key
    where
        master_event_type.event_id in (112700009, 112700010)
        and event_stat is null
),
ind_fib_times as (
    select
        visit_key,
{%- for i in range(1, 5+1) %}
        max(case when event_id = 112700009 and rowno = {{i}} then event_dt end)
            as induced_fibrillation_start_date_{{i}},
        max(case when event_id = 112700010 and rowno = {{i}} then event_dt end)
            as induced_fibrillation_stop_date_{{i}},
{%- endfor %}
        min(case when event_id = 112700009 then event_dt end) as first_induced_fibrillation_start_date,
        max(case when event_id = 112700010 then event_dt end) as last_induced_fibrillation_stop_date
    from
        induced_fibrillation
    group by
        visit_key
),
final_times as (
    select
        stg_encounter.visit_key,
        stg_patient.mrn,
        stg_patient.patient_name,
        stg_patient.sex,
        stg_patient.dob,
        stg_encounter.csn,
        ind_fib_times.first_induced_fibrillation_start_date,
        ind_fib_times.last_induced_fibrillation_stop_date,
        ind_fib_times.induced_fibrillation_start_date_1,
        ind_fib_times.induced_fibrillation_stop_date_1,
    {%- for i in range(2, 5+1) %} --noqa: PRS
        case
            when ind_fib_times.induced_fibrillation_stop_date_{{i}} is null
            then null
            else ind_fib_times.induced_fibrillation_start_date_{{i}} --noqa: PRS
        end as induced_fibrillation_start_date_{{i}},
        case
            when ind_fib_times.induced_fibrillation_start_date_{{i}} is null
            then null
            else ind_fib_times.induced_fibrillation_stop_date_{{i}} --noqa: PRS
        end as induced_fibrillation_stop_date_{{i}}, --noqa: PRS
    {%- endfor %}
        coalesce(extract(epoch from ind_fib_times.induced_fibrillation_stop_date_1
                   - ind_fib_times.induced_fibrillation_start_date_1) / 60, 0)
            + coalesce(extract(epoch from ind_fib_times.induced_fibrillation_stop_date_2
                - ind_fib_times.induced_fibrillation_start_date_2) / 60, 0)
            + coalesce(extract(epoch from ind_fib_times.induced_fibrillation_stop_date_3
                - ind_fib_times.induced_fibrillation_start_date_3) / 60, 0)
            + coalesce(extract(epoch from ind_fib_times.induced_fibrillation_stop_date_4
               - ind_fib_times.induced_fibrillation_start_date_4) / 60, 0)
            + coalesce(extract(epoch from ind_fib_times.induced_fibrillation_stop_date_5
               - ind_fib_times.induced_fibrillation_start_date_5) / 60, 0)
        as total_induced_fibrillation_minutes
    from
        ind_fib_times
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = ind_fib_times.visit_key
        inner join {{ref('stg_patient')}} as stg_patient
            on stg_patient.pat_key = stg_encounter.pat_key
)
select
    visit_key,
    mrn,
    patient_name,
    sex,
    dob,
    csn,
    first_induced_fibrillation_start_date,
    last_induced_fibrillation_stop_date,
{%- for i in range(1, 5+1) %} --noqa: PRS
    induced_fibrillation_start_date_{{i}},
    induced_fibrillation_stop_date_{{i}},
{%- endfor %}
    case
        when total_induced_fibrillation_minutes < 0
            then extract(epoch from last_induced_fibrillation_stop_date
                        - first_induced_fibrillation_start_date) / 60
        else total_induced_fibrillation_minutes
    end as total_induced_fibrillation_minutes
from
    final_times
