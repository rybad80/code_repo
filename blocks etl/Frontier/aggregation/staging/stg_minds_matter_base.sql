with
agg_base as (--region
    select
        visit_key,
        'Minds Matter Specialty Care Patients' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_minds_matter_encounter_cohort') }}
    where
        encounter_sub_group = 'Specialty Care'
    union all
    select
        visit_key,
        'Minds Matter ED & Urgent Care Patients' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_minds_matter_encounter_cohort') }}
    where
        encounter_sub_group = 'ED/UC'
    union all
    select
        visit_key,
        'Minds Matter Care Network PCP Patients' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_minds_matter_encounter_cohort') }}
    where
        encounter_sub_group = 'Primary Care'
    union all
    select
        visit_key,
        'Other Concussion Patients' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_minds_matter_encounter_cohort') }}
    where
        encounter_sub_group = 'Other'
    group by
        visit_key,
        pat_key
    --end region
)
select
    'Minds Matter' as program_name,
    sub_cohort,
    frontier_minds_matter_encounter_cohort.mrn,
    frontier_minds_matter_encounter_cohort.pat_key,
    agg_base.visit_key,
    'Geo' as metric_name,
    case
        when frontier_minds_matter_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6))
        then 'FYTD'
        when frontier_minds_matter_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6)) - 1
                and frontier_minds_matter_encounter_cohort.encounter_date
                    < date(add_months(current_date, - 12))
        then 'PFYTD'
        when frontier_minds_matter_encounter_cohort.encounter_date
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
    inner join {{ ref('frontier_minds_matter_encounter_cohort')}} as frontier_minds_matter_encounter_cohort
        on agg_base.visit_key = frontier_minds_matter_encounter_cohort.visit_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on frontier_minds_matter_encounter_cohort.pat_key = stg_patient.pat_key
where
    encounter_date < current_date
