{{ config(materialized='table', dist='pat_key') }}

with oth_resp_support as (
    select
        stg_outbreak_pui_cohort_union.pat_key,
        stg_outbreak_pui_fs_vent_lim.resp_o2_device,
        row_number() over (
            partition by stg_outbreak_pui_cohort_union.pat_key
            order by (
                stg_outbreak_pui_cohort_union.min_specimen_taken_date
                - stg_outbreak_pui_fs_vent_lim.encounter_date
            )
        ) as most_recent
    from
        {{ ref('stg_outbreak_pui_cohort_union') }} as stg_outbreak_pui_cohort_union
        inner join {{ ref('stg_outbreak_pui_fs_vent_lim') }} as stg_outbreak_pui_fs_vent_lim
            on stg_outbreak_pui_cohort_union.pat_key = stg_outbreak_pui_fs_vent_lim.pat_key
    where
        stg_outbreak_pui_fs_vent_lim.resp_o2_device is not null
        and stg_outbreak_pui_fs_vent_lim.resp_o2_device not in (
            'ventilation~ invasive',
            'not applicable',
            'invasive ventilation'
        )
        and stg_outbreak_pui_fs_vent_lim.encounter_date
            between stg_outbreak_pui_cohort_union.min_specimen_taken_date_pre30d
            and stg_outbreak_pui_cohort_union.min_specimen_taken_date_post30d
),

flowsheet_indicators as (
    select
        stg_outbreak_pui_cohort_union.pat_key,
        stg_outbreak_pui_cohort_union.outbreak_type,
        max(
            case
                when stg_outbreak_pui_fs_all_lim.flowsheet_id in (40000824, 40000825, 40003500)
                and (
                    stg_outbreak_pui_fs_all_lim.meas_val is not null
                    or stg_outbreak_pui_fs_all_lim.meas_val_num is not null
                )
                then 1
                else 0
            end
        ) as ecmo_yn,
        max(
            case
                when coalesce(
                    flowsheet_vitals.temperature_c,
                    flowsheet_vitals.secondary_temperature_c
                ) > 38
                then 1
                else 0
            end
        ) as fever_yn,
        max(
            case
                when stg_outbreak_pui_fs_vent_lim.resp_o2_device in (
                    'ventilation~ invasive',
                    'invasive ventilation'
                )
                and stg_outbreak_pui_fs_vent_lim.invasive_ind = 1
                then 1
                else 0
            end
        ) as mechvent_yn,
        min(
            case
                when stg_outbreak_pui_fs_vent_lim.resp_o2_device in (
                    'ventilation~ invasive',
                    'invasive ventilation'
                )
                and stg_outbreak_pui_fs_vent_lim.invasive_ind = 1
                then stg_outbreak_pui_fs_vent_lim.recorded_date
                else null
            end
        ) as min_mv,
        max(
            case
                when stg_outbreak_pui_fs_vent_lim.resp_o2_device in (
                    'ventilation~ invasive',
                    'invasive ventilation'
                )
                and stg_outbreak_pui_fs_vent_lim.invasive_ind = 1
                then stg_outbreak_pui_fs_vent_lim.recorded_date
                else null
            end
        ) as max_mv,
        max(
            case
                when stg_outbreak_pui_fs_all_lim.meas_val like '%feverish%'
                then 1
                else 0
            end
        ) as sfever_yn,
        max(
            case
                when regexp_like(stg_outbreak_pui_fs_all_lim.meas_val, '(?<!no )chills')
                then 1
                else 0
            end
        ) as chills_yn,
        max(
            case
                when stg_outbreak_pui_fs_all_lim.flowsheet_id in ('17252', '10701')
                and stg_outbreak_pui_fs_all_lim.meas_val like '%muscl%'
                then 1
                else 0
            end
        ) as myalgia_yn,
        max(
            case
                when stg_outbreak_pui_fs_all_lim.flowsheet_id = '40068118'
                and stg_outbreak_pui_fs_all_lim.meas_val not like '%unable to assess%'
                then 1
                else 0
            end
        ) as runnose_yn,
        max(
            case
                when stg_outbreak_pui_fs_all_lim.flowsheet_id = '17252'
                and stg_outbreak_pui_fs_all_lim.meas_val like '%throat%'
                then 1
                else 0
            end
        ) as sthroat_yn,
        max(
            case
                when stg_outbreak_pui_fs_all_lim.flowsheet_id = '40068107'
                or (
                    stg_outbreak_pui_fs_all_lim.flowsheet_id = '10701'
                    and stg_outbreak_pui_fs_all_lim.meas_val like '%cough%'
                )
                then 1
                else 0
            end
        ) as cough_yn,
        max(
            case
                when regexp_like(stg_outbreak_pui_fs_all_lim.meas_val, '(?<!no )(?<!denies )shortness of breath')
                then 1
                else 0
            end
        )  as sob_yn,
        max(
            case
                when stg_outbreak_pui_fs_all_lim.flowsheet_id = '3008869'
                or (
                    stg_outbreak_pui_fs_all_lim.flowsheet_id = '10701'
                    and stg_outbreak_pui_fs_all_lim.meas_val like '%vomit%'
                )
                then 1
                else 0
            end
        ) as nauseavomit_yn,
        max(
            case
                when stg_outbreak_pui_fs_all_lim.flowsheet_id in ('17252', '10701')
                and stg_outbreak_pui_fs_all_lim.meas_val like '%head%'
                and stg_outbreak_pui_fs_all_lim.meas_val not like '%light-headed%'
                and stg_outbreak_pui_fs_all_lim.meas_val not like '%no head%'
                then 1
                else 0
            end
        ) as headache_yn,
        max(
            case
                when (
                    stg_outbreak_pui_fs_all_lim.flowsheet_id = '17252'  -- pain location flowsheet
                    and stg_outbreak_pui_fs_all_lim.meas_val like '%abdo%'
                )
                or (
                    stg_outbreak_pui_fs_all_lim.flowsheet_id = '40061062'  -- Abdominal Assessment
                    and stg_outbreak_pui_fs_all_lim.meas_val like '%tender%'
                )
                or (
                    stg_outbreak_pui_fs_all_lim.flowsheet_id = '10701'  -- symptoms flowsheet
                    and stg_outbreak_pui_fs_all_lim.meas_val like '%abdominal%'
                )
                then 1
                else 0
            end
        ) as abdom_yn,
        max(
            case
                when (
                    stg_outbreak_pui_fs_all_lim.flowsheet_name like '%stool appearance%'
                    and regexp_like(
                        stg_outbreak_pui_fs_all_lim.meas_val,
                        'completely unformed|'
                        || 'partially formed|'
                        || 'watery|'
                        || 'loose|'
                        || 'mucus'
                    )
                )
                or (
                    stg_outbreak_pui_fs_all_lim.flowsheet_id = '10701'
                    and stg_outbreak_pui_fs_all_lim.meas_val like '%diarrhea%'
                )
                then 1
                else 0
            end
        ) as diarrhea_yn
    from
        {{ ref('stg_outbreak_pui_cohort_union') }} as stg_outbreak_pui_cohort_union
        inner join {{ ref('stg_outbreak_pui_fs_all_lim') }} as stg_outbreak_pui_fs_all_lim
            on stg_outbreak_pui_cohort_union.pat_key = stg_outbreak_pui_fs_all_lim.pat_key
        left join {{ref('flowsheet_vitals')}} as flowsheet_vitals
            on flowsheet_vitals.flowsheet_record_id = stg_outbreak_pui_fs_all_lim.flowsheet_record_id
            and flowsheet_vitals.recorded_date = stg_outbreak_pui_fs_all_lim.recorded_date
        left join {{ ref('stg_outbreak_pui_fs_vent_lim') }} as stg_outbreak_pui_fs_vent_lim
            on stg_outbreak_pui_fs_vent_lim.flowsheet_record_id = stg_outbreak_pui_fs_all_lim.flowsheet_record_id
            and stg_outbreak_pui_fs_vent_lim.recorded_date = stg_outbreak_pui_fs_all_lim.recorded_date
    where
        stg_outbreak_pui_fs_all_lim.recorded_date
            between stg_outbreak_pui_cohort_union.min_specimen_taken_date_pre30d
            and stg_outbreak_pui_cohort_union.min_specimen_taken_date_post30d
    group by
        stg_outbreak_pui_cohort_union.pat_key,
        stg_outbreak_pui_cohort_union.outbreak_type
)

select
    flowsheet_indicators.pat_key,
    flowsheet_indicators.outbreak_type,
    flowsheet_indicators.ecmo_yn,
    flowsheet_indicators.fever_yn,
    flowsheet_indicators.mechvent_yn,
    flowsheet_indicators.min_mv,
    flowsheet_indicators.max_mv,
    flowsheet_indicators.sfever_yn,
    flowsheet_indicators.chills_yn,
    flowsheet_indicators.myalgia_yn,
    flowsheet_indicators.runnose_yn,
    flowsheet_indicators.sthroat_yn,
    flowsheet_indicators.cough_yn,
    flowsheet_indicators.sob_yn,
    flowsheet_indicators.nauseavomit_yn,
    flowsheet_indicators.headache_yn,
    flowsheet_indicators.abdom_yn,
    flowsheet_indicators.diarrhea_yn,
    case
        when oth_resp_support.pat_key is not null
        then 1
        else 0
    end as oth_resp_support
from
    {{ ref('stg_outbreak_pui_cohort_union') }} as stg_outbreak_pui_cohort_union
    left join flowsheet_indicators
        on stg_outbreak_pui_cohort_union.pat_key = flowsheet_indicators.pat_key
        and stg_outbreak_pui_cohort_union.outbreak_type = flowsheet_indicators.outbreak_type
    left join oth_resp_support
        on flowsheet_indicators.pat_key = oth_resp_support.pat_key
        and oth_resp_support.most_recent = 1
where
    flowsheet_indicators.pat_key is not null
