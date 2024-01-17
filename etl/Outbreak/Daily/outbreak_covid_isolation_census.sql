{{ config(meta = {
    'critical': true
}) }}

/*Midnight census of isolation units*/
with census as (
    select
        master_date.full_dt as census_date,
        outbreak_covid_isolation.*
    from
        {{source('cdw', 'master_date')}} as master_date
        cross join {{ref('outbreak_covid_isolation')}}
            as outbreak_covid_isolation
    where
        master_date.full_dt >= '2020-3-16'
        and master_date.full_dt between outbreak_covid_isolation.enter_date
        and coalesce(outbreak_covid_isolation.exit_date, current_date)
),

covid_pos as (
    select distinct
        census.census_date,
        census.visit_key
    from
        census
        inner join {{ref('stg_outbreak_covid_active_infections')}}
            as stg_outbreak_covid_active_infections
            on stg_outbreak_covid_active_infections.pat_key = census.pat_key
    where
        census.census_date
        between stg_outbreak_covid_active_infections.start_date
        and coalesce(stg_outbreak_covid_active_infections.end_date,
        current_date)
),

precautions as (
    select
        census.census_date,
        census.visit_key,
        max(case when dim_patient_isolation.patient_isolation_id = 8
            then 1 else 0 end) as expanded_ind,
        max(case when dim_patient_isolation.patient_isolation_id = 13
            then 1 else 0 end) as modified_expanded_ind
    from
        census
        inner join {{source('cdw', 'hospital_isolation')}}
            as hospital_isolation
            on hospital_isolation.visit_key = census.visit_key
        inner join {{source('cdw', 'dim_patient_isolation')}}
            as dim_patient_isolation
            on dim_patient_isolation.dim_patient_isolation_key
            = hospital_isolation.dim_patient_isolation_key
        where
            dim_patient_isolation.patient_isolation_id in (
                8,	--EXPANDED PRECAUTIONS
                13) --MODIFIED EXPANDED PRECAUTIONS
            and census.census_date
            between hospital_isolation.isolation_added_dt
            and coalesce(hospital_isolation.isolation_removed_dt, current_date)
    group by
        census.census_date,
        census.visit_key
)

select
    census.census_date,
    census.visit_key,
    census.patient_name,
    census.mrn,
    census.dob,
    census.csn,
    census.encounter_date,
    census.unit_name,
    case when covid_pos.visit_key is null then 0 else 1 end as covid_positive_ind,
    case
        when precautions.expanded_ind = 1
            then 'Expanded Precautions'
        when precautions.modified_expanded_ind = 1
            then 'Modified Expanded Precautions'
            else 'None'
    end as precautions,
    census.pat_key,
    census.hsp_acct_key
from
    census
    left join covid_pos on
        covid_pos.census_date = census.census_date
        and covid_pos.visit_key = census.visit_key
    left join precautions on
        precautions.census_date = census.census_date
        and precautions.visit_key = census.visit_key
