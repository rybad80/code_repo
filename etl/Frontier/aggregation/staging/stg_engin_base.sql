with
agg_base as (--region
    select
        primary_key as visit_key,
        'inpatient' as sub_cohort,
        num as pat_key
    from
        {{ ref('stg_frontier_engin_build_g_ip_patient') }}
    union all
    select
        primary_key as visit_key,
        'outpatient' as sub_cohort,
        num as pat_key
    from
        {{ ref('stg_frontier_engin_build_g_op_patient') }}
    group by
        primary_key,
        num
    --end region
)
select
    'ENGIN' as program_name,
    sub_cohort,
    frontier_engin_encounter_cohort.mrn,
    frontier_engin_encounter_cohort.pat_key,
    agg_base.visit_key,
    'Geo' as metric_name,
    case
        when frontier_engin_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6))
        then 'FYTD'
        when frontier_engin_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6)) - 1
                and frontier_engin_encounter_cohort.encounter_date
                    < date(add_months(current_date, - 12))
        then 'PFYTD'
        when frontier_engin_encounter_cohort.encounter_date
                    < date(current_date)
        then 'Total Patients (All Fiscal Years)' end
    as metric_level,
    case
        when regexp_like(lower(visit_type), 'video visit|'
                                            || 'telephone visit')
            or lower(encounter_type) like '%telemedicine%'
        then 'Digital Visit'
        when lower(encounter_type) like '%office visit%' then 'Office Visit'
        when lower(encounter_type) like '%hospital encounter%' then 'Hospital Encounter'
        else 'Default' end
    as visit_cat,
    initcap(mailing_state) as mailing_state,
    initcap(mailing_city) as mailing_city,
    1 as num
from agg_base
    inner join {{ ref('frontier_engin_encounter_cohort')}} as frontier_engin_encounter_cohort
        on agg_base.visit_key = frontier_engin_encounter_cohort.visit_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on frontier_engin_encounter_cohort.pat_key = stg_patient.pat_key
where
    encounter_date < current_date
