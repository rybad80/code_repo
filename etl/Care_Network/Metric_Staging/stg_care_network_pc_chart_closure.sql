with main as (
    select
        prov_key,
        dim_date.fiscal_year,
        1 as encounter_ind,
        case
            when encounter_days_to_close between 0 and 5 then 1
            else 0
        end as five_day_ind,
        case
            when encounter_closed_ind = 0 then 1
        end as encounter_still_open_ind
    from {{ ref('care_network_expected_event') }} as care_network_expected_event
    inner join {{ ref('dim_department') }} as dim_department
        on dim_department.department_id = care_network_expected_event.department_id
    inner join {{ ref('dim_date') }} as dim_date
        on dim_date.full_date = care_network_expected_event.encounter_date
    where encounter_type_id in (101, 50)    -- encounter type of 'Office Visit' or 'Appointment'
    and appointment_status_id in (2, 6)     -- appointment status of 'Completed' or 'Arrived'
    and erroneous_encounter_ind = 0
    and provider_ind = 1
    and visit_provider_type != 'Resource'
    and dim_department.intended_use_name = 'Primary Care'
    and dim_department.department_id != 89296012 -- department_name <> 'MKT 3550 CN CHOP CAMP'
    and encounter_date between '2022-07-01' and last_day(add_months(current_date, -1))
    -- adding in for safety in case the query is run before the 6th of the month
    and encounter_date <= (current_date - 6)
)

select
    stg_care_network_distinct_worker.provider_worker_id,
    stg_care_network_distinct_worker.provider_last_name,
    stg_care_network_distinct_worker.provider_first_name,
    stg_care_network_distinct_worker.provider_middle_initial,
    main.fiscal_year,
    sum(five_day_ind) as charts_completed,
    coalesce(sum(encounter_still_open_ind), 0) as charts_open,
    sum(encounter_ind) as fytd_total_encounters
from main
inner join {{ ref('stg_care_network_distinct_worker') }} as stg_care_network_distinct_worker
    on main.prov_key = stg_care_network_distinct_worker.prov_key
where lower(stg_care_network_distinct_worker.provider_type) = 'physician'
group by
    provider_worker_id,
    provider_last_name,
    provider_first_name,
    provider_middle_initial,
    fiscal_year
