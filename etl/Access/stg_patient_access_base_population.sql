{{ config(materialized='table', dist='csn') }}

--The purpose of this query is to pull in all relevant visits and data about those visits and patients that
--will be used in this app. This table is used later to join in metrics. 
select
    csn,
    mrn,
    encounter_date as contact_date,
    date_trunc('month', encounter_date) as month_year,
    case when extract(month from encounter_date) between 7 and 12 then extract(year from encounter_date) + 1
        else extract(year from encounter_date)
    end as fiscal_year,
    department_name,
    specialty,
    dept_cntr as department_center,
    loc_nm as revenue_location,
    lookup_access_primary_specialty_care_departments.specialty_care_ind,
    lookup_access_primary_specialty_care_departments.primary_care_ind
from
    {{ref('stg_encounter')}} as stg_encounter
    inner join {{source('cdw', 'department')}} as department
        on department.dept_key = stg_encounter.dept_key
    left join {{source('cdw', 'location')}} as location --noqa: L029
        on location.loc_key = department.rev_loc_key
    left join
        {{ref('lookup_access_primary_specialty_care_departments')}} as lookup_access_primary_specialty_care_departments --noqa: L016
        on department.dept_id = lookup_access_primary_specialty_care_departments.department_id
where
    encounter_date < current_date
    and stg_encounter.department_id not in (
        101001609,
        101026004,
        101026006,
        1015002,
        101001099,
        101026002,
        101012050,
        101012151,
        101001025,
        10011062,
        101033100
)
