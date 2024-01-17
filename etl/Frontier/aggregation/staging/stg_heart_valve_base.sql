with
agg_base as (--region
    select
        visit_key,
        'All CHOP Heart Valve Patients' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_heart_valve_encounter_cohort') }}
    group by
        visit_key,
        pat_key
    --end region
)
select
    'Heart Valve' as program_name,
    sub_cohort,
    frontier_heart_valve_encounter_cohort.mrn,
    frontier_heart_valve_encounter_cohort.pat_key,
    agg_base.visit_key,
    'Geo' as metric_name,
    case
        when frontier_heart_valve_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6))
        then 'FYTD'
        when frontier_heart_valve_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6)) - 1
                and frontier_heart_valve_encounter_cohort.encounter_date
                    < date(add_months(current_date, - 12))
        then 'PFYTD'
        when frontier_heart_valve_encounter_cohort.encounter_date
                    < date(current_date)
        then 'Total Patients (All Fiscal Years)' end
    as metric_level,
    case
        when frontier_heart_valve_encounter_cohort.heart_valve_notes_ind = '1' then 'Conference Note'
        when lower(stg_encounter.encounter_type) like '%office visit%' then 'Office Visit'
        when lower(stg_encounter.encounter_type) like '%hospital encounter%' then 'Hospital Encounter'
        else 'Default' end
    as visit_cat,
    initcap(mailing_state) as mailing_state,
    initcap(mailing_city) as mailing_city,
    1 as num
from agg_base
    inner join {{ ref('frontier_heart_valve_encounter_cohort')}} as frontier_heart_valve_encounter_cohort
        on agg_base.visit_key = frontier_heart_valve_encounter_cohort.visit_key
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on frontier_heart_valve_encounter_cohort.visit_key = stg_encounter.visit_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on frontier_heart_valve_encounter_cohort.pat_key = stg_patient.pat_key
where
    stg_encounter.encounter_date < current_date
