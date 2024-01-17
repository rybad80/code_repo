with
agg_base as (--region
    select
        visit_key,
        'Ec' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_food_allergy_encounter_cohort') }}
    where
        ec_ind = 1
    union all
    select
        visit_key,
        'Eg' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_food_allergy_encounter_cohort') }}
    where
        eg_ind = 1
    union all
    select
        visit_key,
        'EoE' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_food_allergy_encounter_cohort') }}
    where
        eoe_ind = 1
        and visit_per_fy_seq_num = 1
    union all
    select
        visit_key,
        'FPIES' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_food_allergy_encounter_cohort') }}
    where
        fpies_ind = 1
    union all
    select
        visit_key,
        'IgE' as sub_cohort,
        pat_key
    from
        {{ ref('frontier_food_allergy_encounter_cohort') }}
    where
        ige_ind = 1
    group by
        visit_key,
        pat_key
    --end region
)
select
    'Food Allergy' as program_name,
    sub_cohort,
    frontier_food_allergy_encounter_cohort.mrn,
    frontier_food_allergy_encounter_cohort.pat_key,
    agg_base.visit_key,
    'Geo' as metric_name,
    case
        when frontier_food_allergy_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6))
        then 'FYTD'
        when frontier_food_allergy_encounter_cohort.fiscal_year
                    = year(add_months(current_date - 1, 6)) - 1
                and frontier_food_allergy_encounter_cohort.encounter_date
                    < date(add_months(current_date, - 12))
        then 'PFYTD'
        when frontier_food_allergy_encounter_cohort.encounter_date
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
    inner join {{ ref('frontier_food_allergy_encounter_cohort')}} as frontier_food_allergy_encounter_cohort
        on agg_base.visit_key = frontier_food_allergy_encounter_cohort.visit_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on frontier_food_allergy_encounter_cohort.pat_key = stg_patient.pat_key
where
    encounter_date < current_date
