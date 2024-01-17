{{ config(meta = {
    'critical': true
}) }}

/*joining pat cohort to transfer cohort to flag months
that fall within an inactive interval*/
with active_patients as (
    select
    stg_care_network_eligible_patients_month_year.pat_key,
        stg_care_network_eligible_patients_month_year.month_year,
        min(
            floor((date(stg_care_network_eligible_patients_month_year.month_year)
            - date(stg_care_network_eligible_patients_month_year.dob)) / 365.25)
        ) as age,
        max(
            case
                when stg_care_network_eligible_patients_month_year.month_year
                > stg_care_network_patient_transfer.inactive_start_dt
                and stg_care_network_eligible_patients_month_year.month_year
                < stg_care_network_patient_transfer.inactive_end_dt
                then 1
                else 0 end
        ) as inactive_ind

    from
        {{ ref('stg_care_network_eligible_patients_month_year')}} as stg_care_network_eligible_patients_month_year
    left join {{ ref('stg_care_network_patient_transfer') }} as stg_care_network_patient_transfer
        on stg_care_network_eligible_patients_month_year.pat_key = stg_care_network_patient_transfer.pat_key

    group by
        stg_care_network_eligible_patients_month_year.pat_key,
        stg_care_network_eligible_patients_month_year.month_year
),

/*final table with active patient indicator*/
active_final as (

    select
        stg_care_network_eligible_patients_month_year.pat_id,
        stg_care_network_eligible_patients_month_year.pat_key,
        stg_care_network_eligible_patients_month_year.mrn,
        stg_care_network_eligible_patients_month_year.dob,
        active_patients.age,
        stg_care_network_eligible_patients_month_year.month_year,
        stg_care_network_eligible_patients_month_year.last_well_date,
        provider.full_nm as pcp_name, -- within past 2 years
        department.dept_id as department_id,
        department.dept_nm as department_name,
        active_patients.age * 12.0 as age_months_enc,
        case
            when provider.prov_id = '0' then null else provider.prov_id
        end as pcp_id,
        /*
        Patients are considered active if they have had
        a provider visit in the previous 2 years,
        are under 22 years old,
        and have not transferred out of the Care Network.
        */
        case
            when
                active_patients.age < 22
                and active_patients.inactive_ind = 0
                then 1
            else 0
        end as pc_active_patient_ind

    from
        {{ ref('stg_care_network_eligible_patients_month_year')}} as stg_care_network_eligible_patients_month_year
    inner join active_patients
        on active_patients.pat_key = stg_care_network_eligible_patients_month_year.pat_key
            and active_patients.month_year = stg_care_network_eligible_patients_month_year.month_year
    left join {{ source('cdw', 'department') }} as department
        on stg_care_network_eligible_patients_month_year.dept_key = department.dept_key
    left join {{ source('cdw', 'provider') }} as provider
        on stg_care_network_eligible_patients_month_year.pcp_key = provider.prov_key
),

/*past well visit count within each age category*/
past_well_visit as (

    select
        pat_key,
        visit_key,
        encounter_date,
        age_category_enc,
        date_trunc(
            'month', encounter_date
        ) + cast('1 month' as interval) as month_year,
        count(
            visit_key
        ) over (
            partition by
                pat_key, age_category_enc
            order by
                encounter_date
            rows between unbounded preceding and current row
        ) as well_cnt_enc

    from
        {{ ref('stg_care_network_patient_month_year')}}

    where
        well_visit_ind = 1
),

/*joining past well visit corresponding to active patient table*/
well_by_month as (

    select
        active_final.pat_id,
        active_final.pat_key,
        active_final.mrn,
        active_final.month_year,
        active_final.dob,
        past_well_visit.age_category_enc,
        past_well_visit.well_cnt_enc,
        (
            (date(active_final.month_year) - date(active_final.dob)) / 365.25
        ) as patient_age_years,
        patient_age_years * 12.0 as patient_age_months,
        case when patient_age_months < 15 then 'less than 15 months'
            when
                patient_age_months >= 15 and patient_age_months < 30
                then 'between 15 and 30 months'
            when
                patient_age_months >= 30 and patient_age_months < 36
                then 'between 30 and 36 months'
            when
                patient_age_months >= 36 then cast(
                    extract(year from age(active_final.month_year, active_final.dob)) as varchar(2)
                ) || ' years'
        end as age_category,
        case when patient_age_months < 15 then 6
            when patient_age_months >= 15 and patient_age_months < 30 then 2
            when patient_age_months >= 36 then 1
        -- well visits needed according to payors
        end as total_well_visit_needed,
        last_value(past_well_visit.well_cnt_enc ignore nulls) over ( --noqa: PRS
            partition by active_final.pat_key, age_category
            order by
                active_final.month_year
            rows between unbounded preceding and 0 preceding
        ) as well_count,
        case
            when well_count >= total_well_visit_needed then 1 else 0
        end as required_well_completed

    from
        active_final
    left join past_well_visit
        on active_final.pat_key = past_well_visit.pat_key
            and active_final.month_year = past_well_visit.month_year

    where
        -- well visits are only monitored for active patients
        active_final.pc_active_patient_ind = 1
        -- asked to exclude this age category by the stakeholders
        and age_category != 'between 30 and 36 months'
),

well_visits_required_completed as (
    select
        active_final.pat_id,
        active_final.pat_key,
        active_final.month_year,
        case
            when max(well_by_month.total_well_visit_needed) >= 1 then 1 else 0
        end as well_visit_needed_ind,
        coalesce(
            max(well_by_month.required_well_completed), 0
        ) as required_well_completed_ind
    from active_final
    left join well_by_month
        on active_final.pat_key = well_by_month.pat_key
            and active_final.month_year = well_by_month.month_year
    group by
        active_final.pat_id,
        active_final.pat_key,
        active_final.month_year
)

select
    active_final.pat_id,
    active_final.pat_key,
    active_final.mrn,
    active_final.month_year,
    active_final.dob,
    active_final.last_well_date,
    active_final.pcp_id,
    active_final.pcp_name,
    active_final.department_id,
    active_final.department_name,
    active_final.pc_active_patient_ind,
    (
        (date(active_final.month_year) - date(active_final.dob)) / 365.25
    ) as age_years,
    age_years * 12.0 as age_months,
    well_visits_required_completed.well_visit_needed_ind,
    well_visits_required_completed.required_well_completed_ind
from
    active_final
left join well_visits_required_completed
    on active_final.pat_key = well_visits_required_completed.pat_key
        and active_final.month_year = well_visits_required_completed.month_year
