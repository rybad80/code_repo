{{ config(meta = {
    'critical': true
}) }}

with supplemental_gas_flowsheets as (
    select
        flowsheet_all.mrn,
        flowsheet_all.visit_key,
        neo_nicu_episode_phl.episode_start_date,
        flowsheet_all.flowsheet_id,
        case
            when flowsheet_all.flowsheet_id = 40002427 then 'iNO'
            when flowsheet_all.flowsheet_id = 40060904 then 'Heliox'
        end as gas_category,
        flowsheet_all.recorded_date,
        flowsheet_all.meas_val_num,
        row_number() over (
            partition by
                flowsheet_all.visit_key
            order by
                flowsheet_all.recorded_date desc
        ) as recorded_order_desc
    from
        {{ ref('flowsheet_all') }} as flowsheet_all
        inner join {{ ref('neo_nicu_episode_phl') }} as neo_nicu_episode_phl
            on neo_nicu_episode_phl.visit_key = flowsheet_all.visit_key
                and flowsheet_all.recorded_date
                between
                neo_nicu_episode_phl.episode_start_date - cast('30 minute' as interval)
                and coalesce(neo_nicu_episode_phl.episode_end_date, current_date) + cast('30 minute' as interval)
    where
        /* Nitric Oxide (ppm), Heliox Total Flow Rate (Lpm)*/
        flowsheet_all.flowsheet_id in (40002427, 40060904)
),

supplemental_gas_stops as (
    select
        visit_key,
        gas_category,
        recorded_date as gas_stop_datetime,
        lag(recorded_date) over (
            partition by visit_key, gas_category
            order by recorded_date) as prev_gas_stop_datetime
    from
        supplemental_gas_flowsheets
    where
        meas_val_num = 0
        /* if gas support didn't reach zero, take the last entry of a visit */
        or recorded_order_desc = 1
),

fact_nicu_supplemental_gases as (
    select
        row_number() over (
            order by
                supplemental_gas_flowsheets.mrn,
                supplemental_gas_flowsheets.episode_start_date
            ) as row_num,
        {{
            dbt_utils.surrogate_key([
                'supplemental_gas_flowsheets.mrn',
                'row_num'
                ])
            }} as nicu_supp_gas_key,
        supplemental_gas_flowsheets.visit_key,
        supplemental_gas_flowsheets.mrn,
        supplemental_gas_flowsheets.episode_start_date,
        supplemental_gas_flowsheets.gas_category,
        min(supplemental_gas_flowsheets.recorded_date) as gas_start_datetime,
        supplemental_gas_stops.gas_stop_datetime,
        max(supplemental_gas_flowsheets.meas_val_num) as max_gas_support
    from
        supplemental_gas_flowsheets
        inner join supplemental_gas_stops
            on supplemental_gas_flowsheets.visit_key = supplemental_gas_stops.visit_key
                and supplemental_gas_flowsheets.gas_category = supplemental_gas_stops.gas_category
                and supplemental_gas_flowsheets.recorded_date
                > coalesce(supplemental_gas_stops.prev_gas_stop_datetime, date('2000-01-01'))
                and supplemental_gas_flowsheets.recorded_date < supplemental_gas_stops.gas_stop_datetime
    group by
        supplemental_gas_flowsheets.mrn,
        supplemental_gas_flowsheets.visit_key,
        supplemental_gas_flowsheets.episode_start_date,
        supplemental_gas_flowsheets.gas_category,
        supplemental_gas_stops.gas_stop_datetime
    having
        /* filter out consecutive entries of '0'*/
        max_gas_support > 0
    order by
        supplemental_gas_flowsheets.mrn,
        supplemental_gas_flowsheets.episode_start_date,
        supplemental_gas_flowsheets.gas_category,
        gas_start_datetime
)
select
    nicu_supp_gas_key,
    visit_key,
    mrn,
    episode_start_date,
    gas_category,
    gas_start_datetime,
    gas_stop_datetime,
    max_gas_support
from
    fact_nicu_supplemental_gases
