with cross_clamp_start_event_used as (
select
     visit_key as visit_key,
     min(event_id) as min_event_id
 from
     {{ref('stg_cardiac_perfusion_cross_clamp_events')}}
where
     event_id in (112700007, 1120000020)
group by
     visit_key
),
crossclamp_start as (
select
      cross_clamp_start_event_used.visit_key as visit_key,
      cc_start_stop.event_dt,
      112700007 as event_id,
      row_number() over (
            partition by cross_clamp_start_event_used.visit_key
            order by cc_start_stop.event_dt
      ) as rowno
 from
     cross_clamp_start_event_used
     inner join {{ref('stg_cardiac_perfusion_cross_clamp_events')}} as cc_start_stop
      on cross_clamp_start_event_used.visit_key = cc_start_stop.visit_key
         and cross_clamp_start_event_used.min_event_id = cc_start_stop.event_id
),
first_crossclamp_start as (
    select
        visit_key,
        event_dt
    from
        crossclamp_start
    where
        rowno = 1
),
crossclamp as (
    select
        cc_start_stop.visit_key,
        cc_start_stop.event_dt,
        cc_start_stop.event_id_new as event_id,
        row_number() over (
            partition by cc_start_stop.visit_key,
                         cc_start_stop.event_id_new
            order by cc_start_stop.event_dt
        ) as rowno
    from
        first_crossclamp_start
        inner join {{ref('stg_cardiac_perfusion_cross_clamp_events')}} as cc_start_stop
            on first_crossclamp_start.visit_key = cc_start_stop.visit_key
    where
        cc_start_stop.event_id_new in (112700007, 112700008, 1120000034, 1120000035)
        and cc_start_stop.event_dt >= first_crossclamp_start.event_dt
),
crossclamp_times as (
    select
        visit_key,
{%- for i in range(1, 5+1) %} --noqa: PRS
        max(case
            when event_id in (112700007, 1120000034) and rowno = {{i}} then event_dt --noqa: PRS
        end) as cross_clamp_start_date_{{i}}, --noqa: PRS
        max(case
            when event_id in (112700008, 1120000035) and rowno = {{i}} then event_dt --noqa: PRS
        end) as cross_clamp_stop_date_{{i}}, --noqa: PRS
{%- endfor %}
        min(case when event_id in (112700007, 1120000034) then event_dt end) as first_cross_clamp_start_date,
        max(case when event_id in (112700008, 1120000035) then event_dt end) as last_cross_clamp_stop_date
    from
        crossclamp
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
        first_cross_clamp_start_date,
        last_cross_clamp_stop_date,
        cross_clamp_start_date_1,
        cross_clamp_stop_date_1,
{%- for i in range(2, 5+1) %} --noqa: PRS
        case
            when cross_clamp_stop_date_{{i}} is null then null else cross_clamp_start_date_{{i}} --noqa: PRS
        end as cross_clamp_start_date_{{i}}, --noqa: PRS
        case
            when cross_clamp_start_date_{{i}} is null then null else cross_clamp_stop_date_{{i}} --noqa: PRS
        end as cross_clamp_stop_date_{{i}}, --noqa: PRS
{%- endfor %}
    coalesce(extract(epoch from cross_clamp_stop_date_1 - cross_clamp_start_date_1) / 60, 0)
            + coalesce(extract(epoch from cross_clamp_stop_date_2 - cross_clamp_start_date_2) / 60, 0)
            + coalesce(extract(epoch from cross_clamp_stop_date_3 - cross_clamp_start_date_3) / 60, 0)
            + coalesce(extract(epoch from cross_clamp_stop_date_4 - cross_clamp_start_date_4) / 60, 0)
            + coalesce(extract(epoch from cross_clamp_stop_date_5 - cross_clamp_start_date_5) / 60, 0)
    as total_cross_clamp_minutes
    from
        crossclamp_times
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = crossclamp_times.visit_key
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
    first_cross_clamp_start_date,
    last_cross_clamp_stop_date,
{%- for i in range(1, 5+1) %} --noqa: PRS
    cross_clamp_start_date_{{i}}, --noqa: PRS
    cross_clamp_stop_date_{{i}}, --noqa: PRS
{%- endfor %}
    case
        when total_cross_clamp_minutes < 0
            then extract(epoch from last_cross_clamp_stop_date - first_cross_clamp_start_date) / 60
        else total_cross_clamp_minutes
    end as total_cross_clamp_minutes
from
    final_times
