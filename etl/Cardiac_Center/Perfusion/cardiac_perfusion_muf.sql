with muf_start as (
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
        master_event_type.event_id in (112700011)
        and event_stat is null
),
first_muf_start as (
    select distinct
        visit_key,
        event_dt
    from
        muf_start
    where
        rowno = 1
),
muf as (
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
        first_muf_start
        inner join {{source('cdw', 'visit_ed_event')}} as visit_ed_event
            on first_muf_start.visit_key = visit_ed_event.visit_key
        inner join {{source('cdw', 'master_event_type')}} as master_event_type
            on visit_ed_event.event_type_key = master_event_type.event_type_key
    where
        master_event_type.event_id in (112700011, 112700012)
        and event_stat is null
),
muf_times as (
    select
        visit_key, 
{%- for i in range(1, 5+1) %}
        max(case when event_id = 112700011 and rowno = {{i}} then event_dt end) as muf_start_date_{{i}},
        max(case when event_id = 112700012 and rowno = {{i}} then event_dt end) as muf_stop_date_{{i}},
{%- endfor %}
        min(case when event_id = 112700011 then event_dt end) as first_muf_start_date,
        max(case when event_id = 112700012 then event_dt end) as last_muf_stop_date
    from
        muf
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
        muf_times.first_muf_start_date,
        muf_times.last_muf_stop_date,
        muf_times.muf_start_date_1,
        muf_times.muf_stop_date_1,
    {%- for i in range(2, 5+1) %} --noqa: PRS
        case
            when muf_stop_date_{{i}} is null then null else muf_start_date_{{i}} --noqa: PRS
        end as muf_start_date_{{i}},
        case
            when muf_start_date_{{i}} is null then null else muf_stop_date_{{i}} --noqa: PRS
        end as muf_stop_date_{{i}}, --noqa: PRS
    {%- endfor %}
        coalesce(extract(epoch from muf_times.muf_stop_date_1 - muf_times.muf_start_date_1) / 60, 0)
            + coalesce(extract(epoch from muf_times.muf_stop_date_2 - muf_times.muf_start_date_2) / 60, 0)
            + coalesce(extract(epoch from muf_times.muf_stop_date_3 - muf_times.muf_start_date_3) / 60, 0)
            + coalesce(extract(epoch from muf_times.muf_stop_date_4 - muf_times.muf_start_date_4) / 60, 0)
            + coalesce(extract(epoch from muf_times.muf_stop_date_5 - muf_times.muf_start_date_5) / 60, 0)
        as total_muf_minutes
    from
        muf_times
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = muf_times.visit_key
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
    first_muf_start_date,
    last_muf_stop_date,
{%- for i in range(1, 5+1) %} --noqa: PRS
    muf_start_date_{{i}},
    muf_stop_date_{{i}},
{%- endfor %}
    case
        when total_muf_minutes < 0
            then extract(epoch from last_muf_stop_date - first_muf_start_date) / 60
        else total_muf_minutes
    end as total_muf_minutes
from
    final_times
