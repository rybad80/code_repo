with numerators as (
    select
        date_trunc('month', stg_pcoti_report_code_events.event_start_date) as event_year_month,
        stg_pcoti_report_code_events.campus_name,
        stg_pcoti_report_code_events.department_group_name,
        sum(
            case
                when stg_pcoti_report_code_events.code_category = 'chop patients (code team activation)'
                then 1
                else 0
            end
        ) as count_codes_patient,
        sum(
            case
                when stg_pcoti_report_code_events.code_category = 'chop non-patients (code team activation)'
                then 1
                else 0
            end
        ) as count_codes_nonpatient,
        sum(
            case
                when stg_pcoti_report_code_events.event_type_abbrev = 'REDCAP_CODE_AA'
                then 1
                else 0
            end
        ) as count_aa,
        count_codes_patient + count_codes_nonpatient + count_aa as count_total
    from
        {{ ref('stg_pcoti_report_code_events') }} as stg_pcoti_report_code_events
    group by
        date_trunc('month', stg_pcoti_report_code_events.event_start_date),
        stg_pcoti_report_code_events.campus_name,
        stg_pcoti_report_code_events.department_group_name
),

denominators as (
    select
        stg_pcoti_metrics_non_icu_patdays.event_year_month,
        stg_pcoti_metrics_non_icu_patdays.campus_name,
        stg_pcoti_metrics_non_icu_patdays.department_group_name,
        stg_pcoti_metrics_non_icu_patdays.denominator_patdays
    from
        {{ ref('stg_pcoti_metrics_non_icu_patdays') }} as stg_pcoti_metrics_non_icu_patdays
),

all_units as (
    select
        campus_name,
        department_group_name
    from
        numerators
    where
        department_group_name is not null

    union

    select
        campus_name,
        department_group_name
    from
        denominators
    where
        department_group_name is not null
),

date_unit_spine as (
    select
        stg_pcoti_metrics_date_spine.event_year_month,
        all_units.campus_name,
        all_units.department_group_name
    from
        {{ ref('stg_pcoti_metrics_date_spine') }} as stg_pcoti_metrics_date_spine
        cross join all_units
)

select
    date_unit_spine.event_year_month,
    date_unit_spine.campus_name,
    date_unit_spine.department_group_name,
    coalesce(numerators.count_codes_patient, 0) as count_codes_patient,
    coalesce(numerators.count_codes_nonpatient, 0) as count_codes_nonpatient,
    coalesce(numerators.count_aa, 0) as count_aa,
    coalesce(numerators.count_total, 0) as count_total,
    coalesce(denominators.denominator_patdays, 0) as denominator_patdays
from
    date_unit_spine
    left join numerators
        on date_unit_spine.event_year_month = numerators.event_year_month
        and date_unit_spine.campus_name = numerators.campus_name
        and date_unit_spine.department_group_name = numerators.department_group_name
    left join denominators
        on date_unit_spine.event_year_month = denominators.event_year_month
        and date_unit_spine.campus_name = denominators.campus_name
        and date_unit_spine.department_group_name = denominators.department_group_name
