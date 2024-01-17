with numerators as (
    select
        date_trunc('month', stg_pcoti_report_ed_to_icu_transfers.icu_enter_date) as event_year_month,
        stg_pcoti_report_ed_to_icu_transfers.from_campus_name as campus_name,
        sum(1) as numerator_transfers
    from
        {{ ref('stg_pcoti_report_ed_to_icu_transfers') }} as stg_pcoti_report_ed_to_icu_transfers
    group by
        date_trunc('month', stg_pcoti_report_ed_to_icu_transfers.icu_enter_date),
        stg_pcoti_report_ed_to_icu_transfers.from_campus_name
),

all_ed_visits as (
    select
        date_trunc('month', encounter_ed.encounter_date) as event_year_month,
        case
            when encounter_ed.initial_ed_department_center_abbr like '%KOP%' then 'KOPH'
            else 'PHL'
        end as campus_name
    from
        {{ ref('encounter_ed') }} as encounter_ed
    where
        encounter_ed.encounter_date >= '2017-01-01'
),

denominators as (
    select
        event_year_month,
        campus_name,
        sum(1) as denom_ed_visits
    from
        all_ed_visits
    group by
        event_year_month,
        campus_name
),

all_campuses as (
    select
        campus_name
    from
        numerators

    union

    select
        campus_name
    from
        denominators
),

date_unit_spine as (
    select
        stg_pcoti_metrics_date_spine.event_year_month,
        all_campuses.campus_name
    from
        {{ ref('stg_pcoti_metrics_date_spine') }} as stg_pcoti_metrics_date_spine
        cross join all_campuses
)

select
    date_unit_spine.event_year_month,
    date_unit_spine.campus_name,
    coalesce(numerators.numerator_transfers, 0) as numerator_transfers,
    coalesce(denominators.denom_ed_visits, 0) as denom_ed_visits
from
    date_unit_spine
    left join numerators
        on date_unit_spine.event_year_month = numerators.event_year_month
        and date_unit_spine.campus_name = numerators.campus_name
    left join denominators
        on date_unit_spine.event_year_month = denominators.event_year_month
        and date_unit_spine.campus_name = denominators.campus_name
