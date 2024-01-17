select
    'Census Tract' as subdiv_type,
    equity_coi2.census_tract_geoid_2010::varchar(50) as subdiv_code,
    equity_coi2.opportunity_score_coi_state_norm as metric
from
    {{ ref('equity_coi2') }} as equity_coi2
    inner join {{ source('cdw', 'census_tract') }} as census_tract
        on equity_coi2.census_tract_geoid_2010 = census_tract.fips
where
    equity_coi2.observation_year = 2015
    and census_tract.stcofips = '42101'
