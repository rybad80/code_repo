/* Query pulling reporting pieces that exist in flowsheets: symptoms, ventilation, and temperature
Looking if these sxs occured within pt's thirty days of start of infection
Final granularity should be one row per infection*/

with flowsheet_vitals as (
-- region identifying if patient had a fever during encounter
    select
        flowsheet_vitals.encounter_key,
        stg_doh_virals_cohort.encounter_episode_key,
        1 as fever_ind
    from
        (
            select
                stg_doh_virals_cohort.encounter_key,
                stg_doh_virals_cohort.encounter_episode_key,
                stg_doh_virals_cohort.placed_date,
                stg_doh_virals_cohort.thirty_day_window
            from {{ ref('stg_doh_virals_cohort') }} as stg_doh_virals_cohort
            where stg_doh_virals_cohort.order_of_tests = 1
        ) as stg_doh_virals_cohort
        inner join {{ ref('flowsheet_vitals')}} as flowsheet_vitals
            on stg_doh_virals_cohort.encounter_key = flowsheet_vitals.encounter_key
            and flowsheet_vitals.recorded_date >= stg_doh_virals_cohort.placed_date
            and flowsheet_vitals.recorded_date <= stg_doh_virals_cohort.thirty_day_window
    where
        coalesce(flowsheet_vitals.temperature_c, flowsheet_vitals.secondary_temperature_c) > 38
    group by
        flowsheet_vitals.encounter_key,
        stg_doh_virals_cohort.encounter_episode_key
-- end region at encounter level
),

flowsheet_ventilation as (
    select
        flowsheet_ventilation.encounter_key,
        stg_doh_virals_cohort.encounter_episode_key,
        1 as mechanical_ventilation_ind
    from
        (
            select
                stg_doh_virals_cohort.encounter_key,
                stg_doh_virals_cohort.encounter_episode_key,
                stg_doh_virals_cohort.placed_date,
                stg_doh_virals_cohort.thirty_day_window
            from {{ ref('stg_doh_virals_cohort') }} as stg_doh_virals_cohort
            where stg_doh_virals_cohort.order_of_tests = 1
        ) as stg_doh_virals_cohort
        inner join {{ ref('flowsheet_ventilation')}} as flowsheet_ventilation
            on stg_doh_virals_cohort.encounter_key = flowsheet_ventilation.encounter_key
            and flowsheet_ventilation.recorded_date >= stg_doh_virals_cohort.placed_date
            and flowsheet_ventilation.recorded_date <= stg_doh_virals_cohort.thirty_day_window
    where
        resp_o2_device in (
            'ventilation~ invasive',
            'invasive ventilation'
        )
    group by
        flowsheet_ventilation.encounter_key,
        stg_doh_virals_cohort.encounter_episode_key
),

flowsheet_all as (
-- region identifying presence of sx that DOH tracks
    select
        stg_doh_virals_cohort.encounter_key,
        stg_doh_virals_cohort.encounter_episode_key,
        max(
            case when
                flowsheet_id in (40000824, 40000825, 40003500)
                and (meas_val is not null or meas_val_num is not null)
            then 1
            else 0
            end
        ) as ecmo_ind,
        max(
            case when meas_val like '%feverish%'
            then 1
            else 0
            end
        ) as subjective_fever_ind,
        max(
            case when regexp_like(meas_val, '(?<!no )chills')
            then 1
            else 0
            end
        ) as chills_ind,
        max(
            case when
                flowsheet_id in ('17252', '10701')
                and meas_val like '%muscl%'
            then 1
            else 0
            end
        ) as myalgia_ind,
        max(
            case when
                flowsheet_id = '40068118'
                and meas_val not like '%unable to assess%'
            then 1
            else 0
            end
        ) as runny_nose_ind,
        max(
            case when
                flowsheet_id = '17252' and meas_val like '%throat%'
            then 1
            else 0
            end
        ) as sore_throat_ind,
         max(
            case when
                flowsheet_id = '40068107'
                or (
                    flowsheet_id = '10701'
                    and meas_val like '%cough%'
                )
            then 1
            else 0
            end
        ) as cough_ind,
        max(
            case when
                regexp_like(meas_val, '(?<!no )(?<!denies )shortness of breath')
            then 1
            else 0
            end
        )  as sob_ind,
        max(
            case when
                flowsheet_id = '3008869'
                or (
                    flowsheet_id = '10701'
                    and meas_val like '%vomit%'
                )
                then 1
                else 0
            end
        ) as nausea_vomit_ind,
        max(
            case when
                flowsheet_id in ('17252', '10701')
                and meas_val like '%head%'
                and meas_val not like '%light-headed%'
                and meas_val not like '%no head%'
            then 1
            else 0
            end
        ) as headache_ind,
        max(
            case when
                (
                    flowsheet_id = '17252'  -- pain location flowsheet
                    and meas_val like '%abdo%'
                ) or (
                    flowsheet_id = '40061062'  -- Abdominal Assessment
                    and meas_val like '%tender%'
                ) or (
                    flowsheet_id = '10701'  -- symptoms flowsheet
                    and meas_val like '%abdominal%'
                )
            then 1
            else 0
            end
        ) as abdominal_pain_ind,
        max(
            case when
                (
                    flowsheet_name like '%stool appearance%'
                    and regexp_like(
                        meas_val,
                        'completely unformed|'
                        || 'partially formed|'
                        || 'watery|'
                        || 'loose|'
                        || 'mucus'
                    )
                )  or (
                    flowsheet_id = '10701'
                    and meas_val like '%diarrhea%'
                )
            then 1
            else 0
        end
        ) as diarrhea_ind
    from
        (
            select
                stg_doh_virals_cohort.encounter_key,
                stg_doh_virals_cohort.encounter_episode_key,
                stg_doh_virals_cohort.placed_date,
                stg_doh_virals_cohort.thirty_day_window
            from {{ ref('stg_doh_virals_cohort') }} as stg_doh_virals_cohort
            where stg_doh_virals_cohort.order_of_tests = 1
        ) as stg_doh_virals_cohort
        inner join {{ ref('flowsheet_all')}} as flowsheet_all
            on stg_doh_virals_cohort.encounter_key = flowsheet_all.encounter_key
            and flowsheet_all.recorded_date >= stg_doh_virals_cohort.placed_date
            and flowsheet_all.recorded_date <= stg_doh_virals_cohort.thirty_day_window
    group by
        stg_doh_virals_cohort.encounter_key,
        stg_doh_virals_cohort.encounter_episode_key
--end region
)

select
    stg_doh_virals_cohort.encounter_key,
    stg_doh_virals_cohort.encounter_episode_key,
    flowsheet_vitals.fever_ind,
    flowsheet_ventilation.mechanical_ventilation_ind,
    flowsheet_all.ecmo_ind,
    flowsheet_all.subjective_fever_ind,
    flowsheet_all.chills_ind,
    flowsheet_all.myalgia_ind,
    flowsheet_all.runny_nose_ind,
    flowsheet_all.sore_throat_ind,
    flowsheet_all.cough_ind,
    flowsheet_all.sob_ind,
    flowsheet_all.nausea_vomit_ind,
    flowsheet_all.headache_ind,
    flowsheet_all.abdominal_pain_ind,
    flowsheet_all.diarrhea_ind
from
    (
            select
                stg_doh_virals_cohort.encounter_key,
                stg_doh_virals_cohort.encounter_episode_key,
                stg_doh_virals_cohort.placed_date,
                stg_doh_virals_cohort.thirty_day_window
            from {{ ref('stg_doh_virals_cohort') }} as stg_doh_virals_cohort
            where stg_doh_virals_cohort.order_of_tests = 1
    ) as stg_doh_virals_cohort
    left join flowsheet_all
        on stg_doh_virals_cohort.encounter_episode_key = flowsheet_all.encounter_episode_key
    left join flowsheet_ventilation
        on stg_doh_virals_cohort.encounter_episode_key = flowsheet_ventilation.encounter_episode_key
    left join flowsheet_vitals
        on stg_doh_virals_cohort.encounter_episode_key = flowsheet_vitals.encounter_episode_key
