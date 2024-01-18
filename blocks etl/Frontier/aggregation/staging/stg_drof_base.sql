with
agg_base as (--region
    select
        primary_key as visit_key,
        drill_down_one as sub_cohort,
        num as pat_key
    from
        {{ ref('stg_frontier_drof_build_g_ip_patient') }}
    group by
        primary_key,
        drill_down_one,
        num
    --end region
)
select
    'DRoF' as program_name,
    agg_base.sub_cohort,
    frontier_drof_encounter_cohort.mrn,
    frontier_drof_encounter_cohort.pat_key,
    agg_base.visit_key,
    'Geo' as metric_name,
    case
        when frontier_drof_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6))
        then 'FYTD'
        when frontier_drof_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6)) - 1
                and frontier_drof_encounter_cohort.encounter_date
                    < date(add_months(current_date, - 12))
        then 'PFYTD'
        when frontier_drof_encounter_cohort.encounter_date
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
    inner join {{ ref('frontier_drof_encounter_cohort')}} as frontier_drof_encounter_cohort
        on agg_base.visit_key = frontier_drof_encounter_cohort.visit_key
    inner join {{ ref('patient_all') }} as patient_all
        on frontier_drof_encounter_cohort.pat_key = patient_all.pat_key
where
    encounter_date < current_date
