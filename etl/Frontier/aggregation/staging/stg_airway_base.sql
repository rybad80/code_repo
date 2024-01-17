with
agg_base as (--region
    select
        primary_key as visit_key,
        'inpatient' as sub_cohort,
        num as pat_key
    from
        {{ ref('stg_frontier_airway_build_g_ip_patient') }}
    union all
    select
        primary_key as visit_key,
        'outpatient' as sub_cohort,
        num as pat_key
    from
        {{ ref('stg_frontier_airway_build_g_op_patient') }}
    group by
        primary_key,
        num
    --end region
)
select
    'Airway' as program_name,
    sub_cohort,
    frontier_airway_encounter_cohort.mrn,
    frontier_airway_encounter_cohort.pat_key,
    agg_base.visit_key,
    'Geo' as metric_name,
    case
        when frontier_airway_encounter_cohort.fiscal_year
                    = year(add_months(current_date, 6))
        then 'FYTD'
        when frontier_airway_encounter_cohort.fiscal_year
                    = year(add_months(current_date, 6)) - 1
                and frontier_airway_encounter_cohort.encounter_date
                    < date(add_months(current_date, - 12))
        then 'PFYTD'
        when frontier_airway_encounter_cohort.encounter_date
                    < date(current_date)
        then 'Total Patients (All Fiscal Years)' end
    as metric_level,
    case
        when frontier_airway_encounter_cohort.visit_type_id in ('2124', --'video visit follow up'
                                                                '2088', -- 'video visit new'
                                                                '2152', -- 'telephone visit'
                                                                '2191', --'video - routine'
                                                                '3081' -- 'video visit airway'
                                                                )
        or lower(frontier_airway_encounter_cohort.encounter_type) like '%telemedicine%'
        then 'Digital Visit'
        when frontier_airway_encounter_cohort.ov_ind = 1 then 'Office Visit'
        when lower(frontier_airway_encounter_cohort.encounter_type) like '%hospital encounter%'
            then 'Hospital Encounter'
            else 'Default' end
    as visit_cat,
    initcap(stg_patient.mailing_state) as mailing_state,
    initcap(stg_patient.mailing_city) as mailing_city,
    1 as num
from
agg_base
    inner join {{ ref('frontier_airway_encounter_cohort')}} as frontier_airway_encounter_cohort
        on agg_base.visit_key = frontier_airway_encounter_cohort.visit_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on frontier_airway_encounter_cohort.pat_key = stg_patient.pat_key
where
    frontier_airway_encounter_cohort.encounter_date < current_date
