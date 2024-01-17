select
    patient_geospatial.pat_key,
    'Census Tract' as subdiv_type,
    patient_geospatial.census_tract_fips as subdiv_code
from
    {{ source('ods', 'patient_geospatial_temp') }} as patient_geospatial
where
    patient_geospatial.census_tract_fips is not null

union all

select
    patient_geospatial.pat_key,
    'ZCTA' as subdiv_type,
    patient_geospatial.zcta_code as subdiv_code
from
    {{ source('ods', 'patient_geospatial_temp') }} as patient_geospatial
where
    patient_geospatial.zcta_code is not null

union all

select
    patient_geospatial.pat_key,
    'Unified School District' as subdiv_type,
    patient_geospatial.unified_school_district_code as subdiv_code
from
    {{ source('ods', 'patient_geospatial_temp') }} as patient_geospatial
where
    patient_geospatial.unified_school_district_code is not null

union all

select
    patient_geospatial.pat_key,
    'Legislative District - Upper' as subdiv_type,
    patient_geospatial.legislative_district_upper_code as subdiv_code
from
    {{ source('ods', 'patient_geospatial_temp') }} as patient_geospatial
where
    patient_geospatial.legislative_district_upper_code is not null

union all

select
    patient_geospatial.pat_key,
    'Legislative District - Lower' as subdiv_type,
    patient_geospatial.legislative_district_lower_code as subdiv_code
from
    {{ source('ods', 'patient_geospatial_temp') }} as patient_geospatial
where
    patient_geospatial.legislative_district_lower_code is not null
