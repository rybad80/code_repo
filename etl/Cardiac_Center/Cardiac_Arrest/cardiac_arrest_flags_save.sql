with epi_aggregated as (
    select
        cardiac_arrest_flags_hourly.cicu_enc_key,
        cardiac_arrest_flags_hourly.time_mark,
        min(case when stg_cardiac_arrest_epi.action_date >= cardiac_arrest_flags_hourly.time_mark
                then stg_cardiac_arrest_epi.action_date
                else null end) as flag_next_epi_admin_date,
        extract(
            epoch from (flag_next_epi_admin_date - cardiac_arrest_flags_hourly.time_mark)
        ) / 3600.0 as time_to_epi_hr
    from
        {{ref('stg_cardiac_arrest_epi')}} as stg_cardiac_arrest_epi
        inner join {{ref('cardiac_arrest_flags_hourly')}} as cardiac_arrest_flags_hourly
            on cardiac_arrest_flags_hourly.cicu_enc_key = stg_cardiac_arrest_epi.cicu_enc_key
    group by
        cardiac_arrest_flags_hourly.cicu_enc_key,
        cardiac_arrest_flags_hourly.time_mark
),

ecmo as (
    select
        cardiac_arrest_flags_hourly.cicu_enc_key,
        cardiac_arrest_flags_hourly.time_mark,
        min(case when stg_cardiac_arrest_ecmo.rec_dt >= cardiac_arrest_flags_hourly.time_mark
            then stg_cardiac_arrest_ecmo.rec_dt
            else null end) as flag_next_ecmo_date,
        extract(
            epoch from (flag_next_ecmo_date - cardiac_arrest_flags_hourly.time_mark)
        ) / 3600.0 as time_to_ecmo_hr
    from
        {{ref('stg_cardiac_arrest_ecmo')}} as stg_cardiac_arrest_ecmo
        inner join {{ref('cardiac_arrest_flags_hourly')}} as cardiac_arrest_flags_hourly
            on cardiac_arrest_flags_hourly.cicu_enc_key = stg_cardiac_arrest_ecmo.cicu_enc_key
    group by
        cardiac_arrest_flags_hourly.cicu_enc_key,
        cardiac_arrest_flags_hourly.time_mark
)

select
    cardiac_arrest_flags_hourly.pat_key,
    cardiac_arrest_flags_hourly.visit_key,
    cardiac_arrest_flags_hourly.cicu_enc_key,
    cardiac_arrest_flags_hourly.time_mark_key,
    cardiac_arrest_flags_hourly.time_mark,
    cardiac_arrest_flags_hourly.lt_30days_ind,
    cardiac_arrest_flags_hourly.risk_cat,
    cardiac_arrest_flags_hourly.next_arrest_date,
    cardiac_arrest_flags_hourly.hrs_to_arrest,
    cardiac_arrest_flags_hourly.huddle_4hr_ind,
    epi_aggregated.flag_next_epi_admin_date,
    epi_aggregated.time_to_epi_hr,
    ecmo.flag_next_ecmo_date,
    ecmo.time_to_ecmo_hr

from
    {{ref('cardiac_arrest_flags_hourly')}} as cardiac_arrest_flags_hourly
    left join epi_aggregated
        on epi_aggregated.cicu_enc_key = cardiac_arrest_flags_hourly.cicu_enc_key
        and epi_aggregated.time_mark = cardiac_arrest_flags_hourly.time_mark
    left join ecmo as ecmo
        on ecmo.cicu_enc_key = cardiac_arrest_flags_hourly.cicu_enc_key
        and ecmo.time_mark = cardiac_arrest_flags_hourly.time_mark
