with wk as (
    select
        year(full_date + 6) as c_yy,
        month(full_date + 6) as c_mm,
        rank() over (partition by year(full_date + 6)  order by full_date + 6 ) as c_wk,
        full_date as wk_start,
        full_date + 6 as wk_end
    from
        {{ref('dim_date')}}
    where
        year(full_date + 6) >= 2013 and day_of_week = 2
),

dates as (
    select
        wk.c_yy,
        wk.c_mm,
        wk.c_wk,
        dim_date.full_date,
        dim_date.weekday_name,
        dim_date.day_of_week,
        wk.wk_start,
        wk.wk_end
    from
        {{ref('dim_date')}} as dim_date
        left join wk on full_date between wk_start and wk_end
    where
        wk.c_yy >= 2013
),

census as (
    select
        capacity_ip_midnight_census.visit_key,
        capacity_ip_midnight_census.midnight_date as census_date,
        dates.wk_start,
        dates.wk_end,
        dates.c_yy,
        dates.c_mm,
        dates.c_wk,
        dates.day_of_week,
        dates.weekday_name,
        capacity_ip_midnight_census.census_dept_key as dept_key,
        capacity_ip_midnight_census.department_group_name,
        capacity_ip_midnight_census.bed_care_group,
        capacity_ip_midnight_census.department_center_abbr,
        capacity_ip_midnight_census.service as midnight_service
    from
        {{ref('capacity_ip_midnight_census')}} as capacity_ip_midnight_census
        left join dates on dates.full_date = capacity_ip_midnight_census.midnight_date
)

select
    stg_capacity_ip_census_features.pat_key,
    census.visit_key,
    census.census_date,
    census.wk_start,
    census.wk_end,
    census.c_yy,
    census.c_mm,
    census.c_wk,
    census.day_of_week as day_of_wk,
    census.weekday_name as day_nm,
    census.dept_key,
    census.department_group_name,
    census.bed_care_group,
    census.midnight_service,
    census.department_center_abbr,
    stg_capacity_ip_census_features.dob,
    stg_capacity_ip_census_features.elective_ind,
    stg_capacity_ip_census_features.admission_source,
    stg_capacity_ip_census_features.inpatient_admit_date,
    stg_capacity_ip_census_features.admission_department,
    stg_capacity_ip_census_features.admission_department_center_abbr,
    stg_capacity_ip_census_features.admission_service,
    stg_capacity_ip_census_features.surgical_admission_service_ind,
    stg_capacity_ip_census_features.ed_ind,
    stg_capacity_ip_census_features.hospital_admit_date,
    stg_capacity_ip_census_features.hospital_discharge_date,
    stg_capacity_ip_census_features.age_years,
    stg_capacity_ip_census_features.age_days,
    stg_capacity_ip_census_features.ethnicity,
    stg_capacity_ip_census_features.mailing_state,
    stg_capacity_ip_census_features.county,
    stg_capacity_ip_census_features.mailing_zip,
    stg_capacity_ip_census_features.chop_market,
    stg_capacity_ip_census_features.international_ind,
    stg_capacity_ip_census_features.ed_dx_group,
    stg_capacity_ip_census_features.ed_dx_subgroup,
    stg_capacity_ip_census_features.surgery_encounter_ind,
    stg_capacity_ip_census_features.emergent_surgery_ind,
    stg_capacity_ip_census_features.surgery_admission_ind,
    stg_capacity_ip_census_features.surgery_admission_location,
    stg_capacity_ip_census_features.viral_encounter_ind,
    stg_capacity_ip_census_features.viral_admission_ind
from census
    inner join {{ref('stg_capacity_ip_census_features')}} as stg_capacity_ip_census_features
        on stg_capacity_ip_census_features.visit_key = census.visit_key
