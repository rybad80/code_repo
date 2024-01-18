with geocoded_address_data_raw as (
    select
        patient_address_geocode.in_singleline as full_address,
        patient_address_geocode.match_addr as geocode_match_address,
        patient_address_geocode.score::float as geocode_match_accuracy_score,
        patient_address_geocode.addr_type as geocode_address_match_type,
        patient_address_geocode.xcoord::numeric(18, 10) as geocode_longitude,
        patient_address_geocode.ycoord::numeric(18, 10) as geocode_latitude,
        trim(patient_address_geocode.side) as geocode_side_of_street,
        trim(patient_address_geocode.nbrhd) as geocode_neighborhood,
        trim(patient_address_geocode.city) as geocode_city,
        trim(patient_address_geocode.metroarea) as geocode_metro_area,
        trim(patient_address_geocode.subregion) as geocode_county,
        trim(patient_address_geocode.region) as geocode_state,
        trim(patient_address_geocode.regionabbr) as geocode_state_abbr,
        trim(patient_address_geocode.postal) as geocode_zip,
        trim(patient_address_geocode.postalext) as geocode_zip_extension,
        trim(patient_address_geocode.statefp_2010) as state_fips_code_2010,
        trim(patient_address_geocode.countyfp_2010) as county_fips_code_2010,
        trim(patient_address_geocode.tractce_2010) as census_tract_fips_code_2010,
        trim(patient_address_geocode.blockce_2010) as census_block_fips_code_2010,
        trim(patient_address_geocode.geoid_2010) as census_block_geoid_2010,
        trim(patient_address_geocode.statefp_2020) as state_fips_code_2020,
        trim(patient_address_geocode.countyfp_2020) as county_fips_code_2020,
        trim(patient_address_geocode.tractce_2020) as census_tract_fips_code_2020,
        trim(patient_address_geocode.blockce_2020) as census_block_fips_code_2020,
        trim(patient_address_geocode.geoid_2020) as census_block_geoid_2020,
        row_number() over (
            partition by patient_address_geocode.in_singleline
            order by census_block_geoid_2020
        ) as api_record_order
    from
        {{ source('ods', 'patient_address_geocode')}} as patient_address_geocode
    where
        patient_address_geocode.addr_type in (
            'PointAddress',
            'StreetAddress',
            'Subaddress',
            'StreetInt'
        )
        and patient_address_geocode.score::float >= 85
),

geocoded_address_data as (
    select
        geocoded_address_data_raw.full_address,
        geocoded_address_data_raw.geocode_match_address,
        geocoded_address_data_raw.geocode_match_accuracy_score,
        geocoded_address_data_raw.geocode_address_match_type,
        geocoded_address_data_raw.geocode_longitude,
        geocoded_address_data_raw.geocode_latitude,
        geocoded_address_data_raw.geocode_side_of_street,
        geocoded_address_data_raw.geocode_neighborhood,
        geocoded_address_data_raw.geocode_city,
        geocoded_address_data_raw.geocode_metro_area,
        geocoded_address_data_raw.geocode_county,
        geocoded_address_data_raw.geocode_state,
        geocoded_address_data_raw.geocode_state_abbr,
        geocoded_address_data_raw.geocode_zip,
        geocoded_address_data_raw.geocode_zip_extension,
        geocoded_address_data_raw.state_fips_code_2010,
        geocoded_address_data_raw.county_fips_code_2010,
        geocoded_address_data_raw.census_tract_fips_code_2010,
        geocoded_address_data_raw.census_block_fips_code_2010,
        geocoded_address_data_raw.census_block_geoid_2010,
        geocoded_address_data_raw.state_fips_code_2020,
        geocoded_address_data_raw.county_fips_code_2020,
        geocoded_address_data_raw.census_tract_fips_code_2020,
        geocoded_address_data_raw.census_block_fips_code_2020,
        geocoded_address_data_raw.census_block_geoid_2020
    from
        geocoded_address_data_raw
    where
        geocoded_address_data_raw.api_record_order = 1
),

clarity_address as (
    select
        pat_addr_chng_hx.pat_id,
        pat_addr_chng_hx.line as address_seq_num,
        pat_addr_chng_hx.eff_start_date as address_effective_start_date,
        lead(pat_addr_chng_hx.eff_start_date, 1) over (
            partition by pat_addr_chng_hx.pat_id
            order by pat_addr_chng_hx.line
        ) as next_eff_start_date,
        case when next_eff_start_date is null then 1 else 0 end as current_address_ind,
        coalesce(next_eff_start_date, current_date) as address_effective_end_date_or_current_date,
        pat_addr_chng_hx.addr_hx_line1 as address_line_1,
        pat_addr_chng_hx.addr_hx_line2 as address_line_2,
        pat_addr_chng_hx.addr_hx_ln_extra as address_line_extra,
        pat_addr_chng_hx.city_hx as address_city,
        zc_state.name as address_state,
        zc_state.abbr as address_state_abbr,
        pat_addr_chng_hx.zip_hx as address_zip,
        zc_country.name as address_country,
        pat_addr_chng_hx.addr_hx_line1
            || ', '
            || coalesce(pat_addr_chng_hx.addr_hx_line2 || ', ', '')
            || coalesce(pat_addr_chng_hx.addr_hx_ln_extra || ', ', '')
            || pat_addr_chng_hx.city_hx
            || ', '
            || zc_state.abbr
            || ' '
            || pat_addr_chng_hx.zip_hx
            as full_address
    from
        {{ source('clarity_ods', 'pat_addr_chng_hx') }} as pat_addr_chng_hx
        left join {{ source('clarity_ods', 'zc_state') }} as zc_state
            on pat_addr_chng_hx.state_hx_c = zc_state.state_c
        left join {{ source('clarity_ods', 'zc_country') }} as zc_country
            on pat_addr_chng_hx.country_c = zc_country.country_c

)

select
    stg_patient.pat_key,
    clarity_address.address_seq_num,
    clarity_address.address_effective_start_date,
    clarity_address.address_effective_end_date_or_current_date,
    clarity_address.current_address_ind,
    clarity_address.address_line_1,
    clarity_address.address_line_2,
    clarity_address.address_line_extra,
    clarity_address.address_city,
    clarity_address.address_state,
    clarity_address.address_state_abbr,
    clarity_address.address_zip,
    clarity_address.address_country,
    clarity_address.full_address,
    case
        when regexp_like(
            lower(clarity_address.full_address),
            'p[ \.]*o[ \.]*box'
        ) then 1
        else 0
    end as po_box_ind,
    case
        when geocoded_address_data.full_address is not null then 1
        else 0
    end as geocode_match_ind,
    geocoded_address_data.geocode_match_address,
    geocoded_address_data.geocode_match_accuracy_score,
    geocoded_address_data.geocode_address_match_type,
    geocoded_address_data.geocode_longitude,
    geocoded_address_data.geocode_latitude,
    geocoded_address_data.geocode_side_of_street,
    geocoded_address_data.geocode_neighborhood,
    geocoded_address_data.geocode_city,
    geocoded_address_data.geocode_metro_area,
    geocoded_address_data.geocode_county,
    geocoded_address_data.geocode_state,
    geocoded_address_data.geocode_state_abbr,
    geocoded_address_data.geocode_zip,
    geocoded_address_data.geocode_zip_extension,
    geocoded_address_data.state_fips_code_2010,
    geocoded_address_data.county_fips_code_2010,
    geocoded_address_data.census_tract_fips_code_2010,
    geocoded_address_data.state_fips_code_2010
        || geocoded_address_data.county_fips_code_2010
        || geocoded_address_data.census_tract_fips_code_2010
        as census_tract_geoid_2010,
    geocoded_address_data.census_block_fips_code_2010,
    geocoded_address_data.census_block_geoid_2010,
    geocoded_address_data.state_fips_code_2020,
    geocoded_address_data.county_fips_code_2020,
    geocoded_address_data.census_tract_fips_code_2020,
    geocoded_address_data.state_fips_code_2020
        || geocoded_address_data.county_fips_code_2020
        || geocoded_address_data.census_tract_fips_code_2020
        as census_tract_geoid_2020,
    geocoded_address_data.census_block_fips_code_2020,
    geocoded_address_data.census_block_geoid_2020,
    trim(municipal_subdivision_xwalk.zcta_geoid) as zcta_geoid,
    trim(municipal_subdivision_xwalk.uni_school_dist_geoid) as unified_school_district_geoid,
    trim(municipal_subdivision_xwalk.uni_school_dist_name) as unified_school_district_name,
    trim(municipal_subdivision_xwalk.legis_dist_upper_geoid) as legislative_district_upper_geoid,
    trim(municipal_subdivision_xwalk.legis_dist_upper_name) as legislative_district_upper_name,
    trim(municipal_subdivision_xwalk.legis_dist_lower_geoid) as legislative_district_lower_geoid,
    trim(municipal_subdivision_xwalk.legis_dist_lower_name) as legislative_district_lower_name,
    trim(municipal_subdivision_xwalk.congress_dist_geoid) as congressional_district_geoid,
    trim(municipal_subdivision_xwalk.congress_dist_name) as congressional_district_name
from
    clarity_address
    inner join {{ ref('stg_patient')}} as stg_patient
        on clarity_address.pat_id = stg_patient.pat_id
    left join geocoded_address_data
        on clarity_address.full_address = geocoded_address_data.full_address
    left join {{ source('manual_ods', 'municipal_subdivision_xwalk') }} as municipal_subdivision_xwalk
        on geocoded_address_data.census_block_geoid_2020 = municipal_subdivision_xwalk.census_block_geoid
where
    clarity_address.address_effective_end_date_or_current_date >= clarity_address.address_effective_start_date
