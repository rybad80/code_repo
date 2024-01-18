{{ config(meta = {
    'critical': true
}) }}

/*appointments for vax admin*/
with appointments as (
    select
        stg_outbreak_covid_vaccination_cohort.pat_id,
        stg_encounter.csn,
        lookup_covid_vaccine_visit_types.dose_description,
        stg_encounter.department_name,
        stg_encounter.appointment_date,
        stg_encounter.cancel_noshow_ind,
        /*add sched date*/
        case when row_number() over (
            partition by stg_outbreak_covid_vaccination_cohort.pat_id,
                lookup_covid_vaccine_visit_types.dose_description
            order by
                stg_encounter.cancel_noshow_ind,
                stg_encounter.appointment_date
        ) = 1
            then 1
        end as first_appointment_ind
    from
        {{ref('stg_outbreak_covid_vaccination_cohort')}}
            as stg_outbreak_covid_vaccination_cohort
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
        inner join {{ref('lookup_covid_vaccine_visit_types')}}
            as lookup_covid_vaccine_visit_types
            on lookup_covid_vaccine_visit_types.visit_type_id
            = stg_encounter.visit_type_id
),

/*First date of vax including external vax*/
any_vax as (
    select
        pat_id,
        /*make this a date since a large number of records will come from
        vax admins with no underlying order that have no time component*/
        min(date(received_date)) as earliest_known_dose_date
    from
        {{ref('stg_outbreak_covid_vaccination_admin')}}
    group by
        pat_id
),

vax_admin_appointment_one_row as (
    select
        stg_patient.mrn,
        stg_patient.patient_name,
        stg_patient.dob,
        stg_outbreak_covid_vaccination_cohort.patient_population,
        stg_outbreak_covid_vaccination_cohort.patient_type,
        first_dose_appt.csn as dose_1_appointment_csn,
        first_dose_appt.appointment_date as dose_1_appointment_date,
        case when first_dose_appt.appointment_date is not null
            then 1 else 0
        end as dose_1_scheduled_ind,
        first_dose_appt.department_name as dose_1_appointment_location,
        first_dose_appt.cancel_noshow_ind as dose_1_cancel_noshow_ind,
        first_dose_vax.received_date as dose_1_received_date,
        case when first_dose_vax.received_date is not null
            then 1 else 0
        end as dose_1_received_ind,
        first_dose_vax.manufacturer_name as dose_1_manufacturer_name,
        second_dose_appt.csn as dose_2_appointment_csn,
        second_dose_appt.appointment_date as dose_2_appointment_date,
        case when second_dose_appt.appointment_date is not null
            then 1 else 0
        end as dose_2_scheduled_ind,
        second_dose_appt.department_name as dose_2_appointment_location,
        second_dose_appt.cancel_noshow_ind as dose_2_cancel_noshow_ind,
        second_dose_vax.received_date as dose_2_received_date,
        case when second_dose_vax.received_date is not null
            then 1 else 0
        end as dose_2_received_ind,
        second_dose_vax.manufacturer_name as dose_2_manufacturer_name,
        case
            when any_vax.pat_id is not null then 1 else 0
        end as any_known_dose_ind,
        any_vax.earliest_known_dose_date,
        stg_patient.pat_key,
        stg_outbreak_covid_vaccination_cohort.pat_id
    from
        {{ref('stg_outbreak_covid_vaccination_cohort')}}
            as stg_outbreak_covid_vaccination_cohort
        inner join {{ref('stg_patient')}} as stg_patient
            on stg_patient.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
        left join appointments as first_dose_appt
            on first_dose_appt.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
            and first_dose_appt.first_appointment_ind = 1
            and first_dose_appt.dose_description = 'First Dose'
        left join appointments as second_dose_appt
            on second_dose_appt.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
            and second_dose_appt.first_appointment_ind = 1
            and second_dose_appt.dose_description = 'Second Dose'
        left join {{ref('stg_outbreak_covid_vaccination_dose')}} as first_dose_vax
            on first_dose_vax.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
            and first_dose_vax.primary_dose_admin_ind = 1
            and first_dose_vax.internal_admin_ind = 1
            and first_dose_vax.dose_description = 'First Dose'
        left join {{ref('stg_outbreak_covid_vaccination_dose')}} as second_dose_vax
            on second_dose_vax.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
            and second_dose_vax.primary_dose_admin_ind = 1
            and second_dose_vax.internal_admin_ind = 1
            and second_dose_vax.dose_description = 'Second Dose'
        left join any_vax on any_vax.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
    where
        coalesce(
            first_dose_appt.pat_id,
            second_dose_appt.pat_id,
            first_dose_vax.pat_id,
            second_dose_vax.pat_id,
            any_vax.pat_id
        ) is not null
),

geographic_info as (
    select
        vax_admin_appointment_one_row.pat_key,
        case
            when lower(patient_address_hist.city) like 'phila%'
            and lower(patient_address_hist.state) = 'pennsylvania'
                then 'PDPH'
            when lower(patient_address_hist.state) = 'pennsylvania'
                then 'PA DOH'
        end as geographic_distributor_group
    from
        vax_admin_appointment_one_row
        inner join {{source('cdw', 'patient_address_hist')}} as patient_address_hist
            on patient_address_hist.pat_key = vax_admin_appointment_one_row.pat_key
    where
        vax_admin_appointment_one_row.patient_population = 'CHOP Patient'
        and date(coalesce(
            vax_admin_appointment_one_row.dose_1_received_date,
            vax_admin_appointment_one_row.dose_2_received_date,
            vax_admin_appointment_one_row.dose_1_appointment_date,
            vax_admin_appointment_one_row.dose_2_appointment_date))
        between patient_address_hist.eff_start_dt
        /*account for future scheduled appts*/
        and coalesce(patient_address_hist.eff_end_dt - interval '1 day', current_date + 90)
)

select
    vax_admin_appointment_one_row.mrn,
    vax_admin_appointment_one_row.patient_name,
    vax_admin_appointment_one_row.dob,
    vax_admin_appointment_one_row.patient_population,
    vax_admin_appointment_one_row.patient_type,
    vax_admin_appointment_one_row.dose_1_appointment_csn,
    vax_admin_appointment_one_row.dose_1_appointment_date,
    vax_admin_appointment_one_row.dose_1_scheduled_ind,
    vax_admin_appointment_one_row.dose_1_appointment_location,
    vax_admin_appointment_one_row.dose_1_cancel_noshow_ind,
    vax_admin_appointment_one_row.dose_1_received_date,
    vax_admin_appointment_one_row.dose_1_received_ind,
    vax_admin_appointment_one_row.dose_1_manufacturer_name,
    vax_admin_appointment_one_row.dose_2_appointment_csn,
    vax_admin_appointment_one_row.dose_2_appointment_date,
    vax_admin_appointment_one_row.dose_2_scheduled_ind,
    vax_admin_appointment_one_row.dose_2_appointment_location,
    vax_admin_appointment_one_row.dose_2_cancel_noshow_ind,
    vax_admin_appointment_one_row.dose_2_received_date,
    vax_admin_appointment_one_row.dose_2_received_ind,
    vax_admin_appointment_one_row.dose_2_manufacturer_name,
    vax_admin_appointment_one_row.any_known_dose_ind,
    vax_admin_appointment_one_row.earliest_known_dose_date,
    patient_all.race_ethnicity,
    patient_all.current_age,
    patient_all.preferred_language,
    patient_all.payor_group,
    case when vax_admin_appointment_one_row.patient_population = 'CHOP Patient'
        then coalesce(geographic_info.geographic_distributor_group, 'Other')
    end as geographic_distributor_group,
    vax_admin_appointment_one_row.pat_key,
    vax_admin_appointment_one_row.pat_id
from
    vax_admin_appointment_one_row
    left join geographic_info
        on geographic_info.pat_key = vax_admin_appointment_one_row.pat_key
    inner join {{ref('patient_all')}} as patient_all
        on patient_all.pat_id = vax_admin_appointment_one_row.pat_id
