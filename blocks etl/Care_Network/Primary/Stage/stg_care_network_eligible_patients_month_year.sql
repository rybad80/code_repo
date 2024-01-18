/*creates a patient level cohort which contains a row for each month-year
a patient was denominator eligible, that is till two years
after a provider visit
*/
with pat_cohort_tmp as (
    select
        stg_care_network_patient_month_year.pat_id,
        stg_care_network_patient_month_year.mrn,
        stg_care_network_patient_month_year.pat_key,
        stg_care_network_patient_month_year.dob,
        stg_care_network_patient_month_year.completed_ind,
        master_date.full_dt as month_year,
        1 as vis_2yr_ind,
        max(stg_care_network_patient_month_year.well_count) as well_count,
        --indicator for the patient-level denominator
        max(stg_care_network_patient_month_year.last_well_date) as last_well_date

    from
        {{ ref('stg_care_network_patient_month_year') }} as stg_care_network_patient_month_year
    inner join {{ source('cdw', 'master_date') }} as master_date
        on date_trunc('month', stg_care_network_patient_month_year.encounter_date) + cast('1 month' as interval)
            between master_date.full_dt - 730 and master_date.full_dt

    where
        master_date.full_dt < current_date
        and master_date.day_of_mm = 1

    group by
        stg_care_network_patient_month_year.mrn,
        stg_care_network_patient_month_year.pat_key,
        stg_care_network_patient_month_year.pat_id,
        stg_care_network_patient_month_year.completed_ind,
        stg_care_network_patient_month_year.dob,
        master_date.full_dt
)

/*
getting each patient's pcp at the time of their most recent cn visit
and the dept of their most recent cn visit
*/


select
    pat_cohort_tmp.pat_id,
    pat_cohort_tmp.mrn,
    pat_cohort_tmp.pat_key,
    pat_cohort_tmp.dob,
    pat_cohort_tmp.well_count,
    pat_cohort_tmp.last_well_date,
    pat_cohort_tmp.completed_ind,
    pat_cohort_tmp.month_year,
    pat_cohort_tmp.vis_2yr_ind,
    stg_care_network_patient_month_year.prov_key,
    stg_care_network_patient_month_year.department_visit,
    stg_care_network_patient_month_year.visit_seq_month,
    /*
    below takes on the value of the last non-null month's pcp for
    a given patient. it is used to fill in the pcp value for months
    in which a patient did not have a visit, but was eligible for
    the denominator
    */
    last_value(stg_care_network_patient_month_year.prov_key ignore nulls) over ( --noqa: PRS
        partition by pat_cohort_tmp.pat_key
        order by
            pat_cohort_tmp.month_year
        rows between unbounded preceding and 1 preceding
    ) as pcp_last,
    coalesce(stg_care_network_patient_month_year.prov_key, pcp_last) as pcp_key,
    last_value(stg_care_network_patient_month_year.department_visit ignore nulls) over ( --noqa: PRS
        partition by pat_cohort_tmp.pat_key
        order by
            pat_cohort_tmp.month_year
        rows between unbounded preceding and 1 preceding
    ) as dept_last,
    coalesce(stg_care_network_patient_month_year.department_visit, dept_last) as dept_key
from
    pat_cohort_tmp
left join {{ ref('stg_care_network_patient_month_year') }} as stg_care_network_patient_month_year
    on pat_cohort_tmp.pat_key = stg_care_network_patient_month_year.pat_key
        and pat_cohort_tmp.month_year = stg_care_network_patient_month_year.month_year
where
    stg_care_network_patient_month_year.visit_seq_month = 1
    or stg_care_network_patient_month_year.visit_seq_month is null
