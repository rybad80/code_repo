with
agg_base as (--region
    select
        visit_key,
        'RBD Patients' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_n_o_t_bleeding_encounter_cohort') }}
    where
        sub_cohort = 'RBD patient'
    union all
    select
        visit_key,
        'GT Patients' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_n_o_t_bleeding_encounter_cohort') }}
    where
        sub_cohort = 'GT patient'
    --end region
)
select
    'NoT Bleeding' as program_name,
    agg_base.sub_cohort,
    frontier_n_o_t_bleeding_encounter_cohort.mrn,
    frontier_n_o_t_bleeding_encounter_cohort.pat_key,
    agg_base.visit_key,
    'Geo' as metric_name,
    case
        when frontier_n_o_t_bleeding_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6))
        then 'FYTD'
        when frontier_n_o_t_bleeding_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6)) - 1
                and frontier_n_o_t_bleeding_encounter_cohort.encounter_date
                    < date(add_months(current_date, - 12))
        then 'PFYTD' end
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
    inner join {{ ref('frontier_n_o_t_bleeding_encounter_cohort')}} as frontier_n_o_t_bleeding_encounter_cohort
        on agg_base.visit_key = frontier_n_o_t_bleeding_encounter_cohort.visit_key
    inner join {{ ref('patient_all') }} as patient_all
        on frontier_n_o_t_bleeding_encounter_cohort.pat_key = patient_all.pat_key
where
    encounter_date < current_date
