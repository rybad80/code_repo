with
base_a_geo as (--region: count = 4,540
    select
        'Geo' as metric_name,
        case
            when frontier_hi_encounter_cohort.fiscal_year >= year(add_months(current_date, 6)) then 'FYTD'
            when
                fiscal_year >= year(add_months(current_date, 6)) - 1
                and encounter_date <= date(add_months(current_date, - 12)) then 'Previous FYTD'
            end as metric_level,
        case
            when lower(visit_type) like '%video visit%'
                or lower(visit_type) like '%telephone visit%'
                or lower(encounter_type) like '%telemedicine%' then 'Digital Visit'
            when lower(encounter_type) like '%office visit%' then 'Office Visit'
            when lower(encounter_type) like '%hospital encounter%' then 'Hospital Encounter'
            else 'Default' end as visit_cat,
        initcap(mailing_state) as mailing_state,
        initcap(mailing_city) as mailing_city,
        1 as num

    from
        {{ ref('frontier_hi_encounter_cohort')}} as frontier_hi_encounter_cohort
        inner join {{ ref('stg_patient') }} as stg_patient
            on frontier_hi_encounter_cohort.pat_key = stg_patient.pat_key

    where
        fiscal_year >= year(add_months(current_date, 6)) - 1

    group by
        metric_level,
        frontier_hi_encounter_cohort.pat_key,
        mailing_state,
        mailing_city,
        visit_type,
        encounter_type

    having metric_level is not null
    --end region

),
base_b_geo as (--region: count = 12,860
    select
        'Geo' as metric_name,
        'Total Cohort' as metric_level,
        case
            when lower(visit_type) like '%video visit%'
                or lower(visit_type) like '%telephone visit%'
                or lower(encounter_type) like '%telemedicine%' then 'Digital Visit'
            when lower(encounter_type) like '%office visit%' then 'Office Visit'
            when lower(encounter_type) like '%hospital encounter%' then 'Hospital Encounter'
            else 'Default' end as visit_cat,
        initcap(mailing_state) as mailing_state,
        initcap(mailing_city) as mailing_city,
        1 as num

    from
        {{ ref('frontier_hi_encounter_cohort')}} as frontier_hi_encounter_cohort
        inner join
            {{ ref('stg_patient') }} as stg_patient on frontier_hi_encounter_cohort.pat_key = stg_patient.pat_key

    group by
        metric_level,
        frontier_hi_encounter_cohort.pat_key,
        mailing_state,
        mailing_city,
        visit_type,
        encounter_type

    having metric_level is not null
    --end region

),
base_geo as (--region: count = 17,400 ✔

    select * from base_a_geo
    union all
    select * from base_b_geo
    --end region

),
vcat_count_geo as (--region:
    select
        metric_name,
        metric_level,
        mailing_state,
        mailing_city,
        visit_cat,
        sum(num) as test,
        row_number()over(partition by metric_name, mailing_state, mailing_city order by test) as row_num

        from base_geo

        where
            mailing_state is not null
            and lower(mailing_state) != 'other'
            and lower(visit_cat) not in ('default')

        group by
            metric_name,
            metric_level,
            mailing_state,
            mailing_city,
            visit_cat
    --end region

),
lat_long_geo as (--region:

    select
        initcap(mailing_city) as mailing_city,
        initcap(mailing_state) as mailing_state,
        lat_deg,
        case when lower(mailing_city) = 'philadelphia' then -75.150843 else long_deg end as long_deg,
        row_number()over(partition by mailing_state, mailing_city order by long_deg) as row_num

    from
        {{ ref('frontier_hi_encounter_cohort')}} as frontier_hi_encounter_cohort
        inner join {{ ref('stg_patient')}} as stg_patient
            on frontier_hi_encounter_cohort.pat_key = stg_patient.pat_key
        inner join {{source('cdw', 'patient')}} as p on frontier_hi_encounter_cohort.pat_key = p.pat_key
        inner join {{source('cdw', 'master_geography')}} as mg on p.geo_key = mg.geo_key

    where
        frontier_hi_encounter_cohort.pat_key is not null

    group by
        mailing_city,
        mailing_state,
        lat_deg,
        long_deg
    --end region

),
combine_geo as (--region:
    select distinct
        vcg.metric_name,
        vcg.metric_level,
        vcg.mailing_state,
        vcg.mailing_city,
        vcg.visit_cat,
        vcg.test as num,
        lat_deg,
        long_deg,
        vcg.row_num

    from
        vcat_count_geo as vcg
        inner join lat_long_geo as llg on vcg.mailing_city = llg.mailing_city
                                            and vcg.mailing_state = llg.mailing_state
                                            and llg.row_num = 1
    --end region

),
stage_geo as (--region:
    select
        metric_name,
        metric_level,
        mailing_state,
        mailing_city,
        visit_cat,
        num,
        row_num,
        case
            when row_num = 2 then ((row_num) * 0.001) + lat_deg
            when row_num = 4 then ((row_num) * (-0.001)) + lat_deg
            else lat_deg end as lat_deg,
        case
            when row_num = 3 then ((row_num) * (-0.001)) + long_deg
            when row_num = 5 then ((row_num) * 0.001) + long_deg
            else long_deg end as long_deg
    from combine_geo
    --end region

)
select
    row_number()over(partition by metric_name order by mailing_city) as rc,
    *

from stage_geo
--;
