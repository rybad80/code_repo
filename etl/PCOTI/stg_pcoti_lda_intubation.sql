with ldas as (
    select
        patient_lda.pat_lda_key,
        patient_lda.lda_id,
        patient_lda.pat_key,
        patient_lda.visit_key,
        patient_lda.place_dt,
        case
            when year(patient_lda.remove_dt) = 2157 then null
            when hour(patient_lda.remove_dt) = 0 and minute(patient_lda.remove_dt) = 0 then null
            else patient_lda.remove_dt
        end as remove_dt
    from
        {{ source('cdw', 'patient_lda') }} as patient_lda
    where
        upper(patient_lda.lda_desc) like '%ENDOTRACHEAL%'
),

anesthesia_events as (
    select
        stg_pcoti_event_surgery.*
    from
        {{ ref('stg_pcoti_event_surgery') }} as stg_pcoti_event_surgery
    where
        stg_pcoti_event_surgery.event_type_abbrev = 'SURG_ANES'
),

missing_ett_place_dates as (
    select
        ldas.pat_lda_key,
        min(flowsheet_ventilation.recorded_date) as invasive_vent_fs_dt
    from
        ldas
        inner join {{ ref('flowsheet_ventilation') }} as flowsheet_ventilation
            on ldas.pat_key = flowsheet_ventilation.pat_key
    where
        -- place date of exactly midnight indicates that placement time field is null in epic
        hour(ldas.place_dt) = 0 and minute(ldas.place_dt) = 0
        and flowsheet_ventilation.resp_o2_device = 'Ventilation~ Invasive'
        -- Keep FS entries on same day as lda placement date and prior to lda removal date;
        -- this gets times for most ETTs. The 100 or so with times outside same day seem
        -- to be ones where patient was intubated by OSH. These xfers are ignored by
        -- our outcome metrics
        and flowsheet_ventilation.recorded_date >= ldas.place_dt
        and flowsheet_ventilation.recorded_date <= (
            case
                when ldas.remove_dt < ldas.place_dt + interval '24 hours' then ldas.remove_dt
                else ldas.place_dt + interval '24 hours'
            end
        )
    group by
        ldas.pat_lda_key
),

-- Identify ETTs placed duiring anesthesia event so we can exclude
ett_procedural as (
    select
        ldas.pat_lda_key
    from
        ldas
        left join missing_ett_place_dates
            on ldas.pat_lda_key = missing_ett_place_dates.pat_lda_key
        inner join anesthesia_events
            on ldas.pat_key = anesthesia_events.pat_key
    where
        coalesce(missing_ett_place_dates.invasive_vent_fs_dt, ldas.place_dt)
        between anesthesia_events.event_start_date
        and anesthesia_events.event_start_date
)

select
    ldas.pat_lda_key,
    ldas.lda_id,
    ldas.pat_key,
    ldas.visit_key,
    coalesce(missing_ett_place_dates.invasive_vent_fs_dt, ldas.place_dt) as place_dt,
    ldas.remove_dt as remove_dt
from
    ldas
    left join missing_ett_place_dates
        on ldas.pat_lda_key = missing_ett_place_dates.pat_lda_key
    left join ett_procedural
        on ldas.pat_lda_key = ett_procedural.pat_lda_key
where
    ett_procedural.pat_lda_key is null
