with geos as (
    select
        patient_geospatial_temp.pat_key,
        equity_coi2.opportunity_lvl_coi_natl_norm
    from
        {{ source('ods', 'patient_geospatial_temp') }} as patient_geospatial_temp
        inner join {{ ref('equity_coi2') }} as equity_coi2
            on patient_geospatial_temp.census_tract_fips = equity_coi2.census_tract_geoid_2010
    where
        equity_coi2.observation_year = 2015
    group by
        patient_geospatial_temp.pat_key,
        equity_coi2.opportunity_lvl_coi_natl_norm
),

pat_encounters as (
    select
        encounter_all.pat_key,
        case
            when geos.opportunity_lvl_coi_natl_norm is null then 'Unknown'
            when geos.opportunity_lvl_coi_natl_norm = 'Very Low' then '(1) Very Low'
            when geos.opportunity_lvl_coi_natl_norm = 'Low' then '(2) Low'
            when geos.opportunity_lvl_coi_natl_norm = 'Moderate' then '(3) Moderate'
            when geos.opportunity_lvl_coi_natl_norm = 'High' then '(4) High'
            when geos.opportunity_lvl_coi_natl_norm = 'Very High' then '(5) Very High'
        end as coi_level,
        case
            when stg_patient.race_ethnicity is null then 'Unknown'
            when stg_patient.race_ethnicity in (
                'Non-Hispanic Black',
                'Non-Hispanic White',
                'Hispanic or Latino',
                'Asian'
            ) then stg_patient.race_ethnicity
            else 'Other'
        end as race_ethnicity,
        case
            when stg_patient.preferred_language is null then 'Unknown'
            when lower(stg_patient.preferred_language) in (
                'english',
                'spanish',
                'mandarin',
                'arabic'
            ) then initcap(stg_patient.preferred_language)
            else 'Other'
        end as preferred_language,
        encounter_all.visit_key,
        encounter_all.encounter_date,
        'FY' || substring(dim_date.fiscal_year::varchar(5), 3, 2) as fiscal_year,
        lookup_equity_payor_details.payor_name_1 as payor_name,
        case
            when lookup_equity_payor_details.line_of_business in (
                'Managed Medicaid',
                'Commercial'
            ) then lookup_equity_payor_details.line_of_business
            else 'Other'
        end as payor_lob,
        case
            when encounter_all.primary_care_ind = 1 then 'Outpatient Primary Care'
            when encounter_all.specialty_care_ind = 1 then 'Outpatient Specialty Care'
            when encounter_all.inpatient_ind = 1 then 'Inpatient Admissions'
            when encounter_all.ed_ind = 1 then 'Emergency Department'
        end as service_type
    from
        {{ ref('encounter_all') }} as encounter_all
        inner join {{ ref('dim_date') }} as dim_date
            on encounter_all.encounter_date = dim_date.full_date
        inner join {{ ref('stg_patient') }} as stg_patient
            on encounter_all.pat_key = stg_patient.pat_key
        left join {{ ref('lookup_equity_payor_details') }} as lookup_equity_payor_details
            on encounter_all.payor_name = lookup_equity_payor_details.payor_name
        left join geos
            on encounter_all.pat_key = geos.pat_key
    where
        dim_date.fiscal_year = 2022
        and (encounter_all.primary_care_ind
        + encounter_all.specialty_care_ind
        + encounter_all.inpatient_ind
        + encounter_all.ed_ind) > 0
)

select
    pat_encounters.coi_level,
    pat_encounters.race_ethnicity,
    pat_encounters.preferred_language,
    pat_encounters.payor_lob,
    pat_encounters.service_type,
    sum(1) as metric
from
    pat_encounters
group by
    pat_encounters.coi_level,
    pat_encounters.race_ethnicity,
    pat_encounters.preferred_language,
    pat_encounters.payor_lob,
    pat_encounters.service_type
