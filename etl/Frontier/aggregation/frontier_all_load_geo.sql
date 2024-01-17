with
vcat_count_geo as (--region:
    select distinct
        program_name,
        metric_name,
        metric_level,
        mailing_state,
        mailing_city,
        visit_cat,
        sum(num) as test,
        row_number() over(
            partition by
                program_name,
                metric_name,
                mailing_state,
                mailing_city
            order by test)
        as row_num
    from
        {{ ref('frontier_all_build_geo') }} as base_geo
    where
        mailing_state is not null
        and lower(mailing_state) != 'other'
        and lower(visit_cat) not in ('default')
    group by
        program_name,
        metric_name,
        metric_level,
        mailing_state,
        mailing_city,
        visit_cat
    --end region
),
lat_long_geo as (--region:
    select distinct
        program_name,
        initcap(base_geo.mailing_city) as mailing_city,
        initcap(base_geo.mailing_state) as mailing_state,
        lat_deg,
        case
            when lower(base_geo.mailing_city) = 'philadelphia' then -75.150843
            else long_deg end
        as long_deg,
        row_number() over(
            partition by
                        program_name,
                        base_geo.mailing_state,
                        base_geo.mailing_city
            order by long_deg)
        as row_num
    from
        {{ ref('frontier_all_build_geo') }} as base_geo
        inner join {{ref ('stg_patient') }} as stg_patient
            on base_geo.pat_key = stg_patient.pat_key
        inner join {{ source('cdw', 'patient') }} as p on base_geo.pat_key = p.pat_key
        inner join {{ source('cdw', 'master_geography') }} as mg on p.geo_key = mg.geo_key
    where
        base_geo.pat_key is not null
    group by
        program_name,
        base_geo.mailing_city,
        base_geo.mailing_state,
        lat_deg,
        long_deg
    --end region
),
combine_geo as (--region:
    select distinct
        llg.program_name,
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
        inner join lat_long_geo as llg
            on vcg.mailing_city = llg.mailing_city
                and vcg.mailing_state = llg.mailing_state
                and llg.row_num = 1
                and vcg.program_name = llg.program_name
    --end region
),
build_geo as (--region:
    select
        program_name,
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
            else lat_deg end
        as lat_deg,
        case
            when row_num = 3 then ((row_num) * (-0.001)) + long_deg
            when row_num = 5 then ((row_num) * 0.001) + long_deg
            else long_deg end
        as long_deg
    from combine_geo
    --end region
)
select
    row_number() over(
        partition by program_name
        order by mailing_city)
    as rc,
    {{dbt_utils.surrogate_key([
            'num',
            'rc',
            'lat_deg'
        ])
    }} as primary_key,
    program_name,
    metric_name,
    metric_level,
    mailing_state,
    mailing_city,
    visit_cat,
    num,
    row_num,
    lat_deg,
    long_deg
from build_geo
