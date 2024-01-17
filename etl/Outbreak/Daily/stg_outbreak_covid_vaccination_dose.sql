{{ config(meta = {
    'critical': true
}) }}

with all_vax as (
    select
        stg_outbreak_covid_vaccination_admin.immune_id,
        stg_outbreak_covid_vaccination_admin.pat_id,
        stg_outbreak_covid_vaccination_admin.received_date,
        stg_outbreak_covid_vaccination_admin.manufacturer_name,
        stg_outbreak_covid_vaccination_admin.dose_description as dose_description_vis_type,
        stg_outbreak_covid_vaccination_admin.admin_source,
        stg_outbreak_covid_vaccination_admin.administration_location,
        stg_outbreak_covid_vaccination_admin.order_id,
        stg_outbreak_covid_vaccination_admin.order_csn,
        stg_outbreak_covid_vaccination_admin.inpatient_administration_ind,
        /*indicator for admins by CHOP*/
        case
            /*non-historical doses...*/
            when stg_outbreak_covid_vaccination_admin.historic_ind = 0
            /*abstracted community clinic doses (manual upload)...*/
            or stg_outbreak_covid_vaccination_admin.comm_clin_abstracted_ind = 1
            /*abstracted community clinic doses (batch upload)...*/
            or (stg_outbreak_covid_vaccination_cohort.patient_population
                = 'Community Clinic Vaccine Recipient')
            /*abstracted doses from HCWs*/
            or (stg_outbreak_covid_vaccination_cohort.patient_population
                = 'Non-affiliated Healthcare Worker'
                and stg_outbreak_covid_vaccination_admin.received_date
                <= '2021-2-23' -- day of HCW conversion to EPIC reporting
                )
            then 1 else 0
        end as internal_admin_ind
    from
        {{ref('stg_outbreak_covid_vaccination_admin')}}
        as stg_outbreak_covid_vaccination_admin
        /*join stage table to id HCW patients*/
        inner join {{ref('stg_outbreak_covid_vaccination_cohort')}}
            as stg_outbreak_covid_vaccination_cohort
            on stg_outbreak_covid_vaccination_cohort.pat_id
            = stg_outbreak_covid_vaccination_admin.pat_id
    where
        stg_outbreak_covid_vaccination_admin.admin_source = 'LPL record'
),

/*Impute dose descriptions (first, second, booster)*/

/*Identify first and second doses based on visit type*/
dose_date_vis_type as (
    select
        pat_id,
        min(
            case when dose_description_vis_type = 'First Dose' then received_date end)
        as first_dose_date,
        min(
            case when dose_description_vis_type = 'Second Dose' then received_date end)
        as second_dose_date,
        min(
            case when dose_description_vis_type = 'Booster Dose' then received_date end)
        as booster_dose_date
    from
        all_vax
    group by
        pat_id
),

/*1. Handle cases where we have some visit type info*/
/*Fill in first dose date if no dose connected to dose 1 visit type*/
fill_first_dose_date as (
    select
        all_vax.immune_id,
        all_vax.pat_id,
        dose_date_vis_type.first_dose_date,
        dose_date_vis_type.second_dose_date,
        dose_date_vis_type.booster_dose_date,
        all_vax.received_date,
        /*dose date is at least a week before second dose if
        second dose identified by vis type*/
        case
            when all_vax.received_date < coalesce(
                dose_date_vis_type.second_dose_date - interval '7 days',
                current_date)
                and dose_date_vis_type.first_dose_date is null
                then 'First Dose'
        end as dose_description_calc_d1,
        /*Update first dose date*/
        coalesce(
            dose_date_vis_type.first_dose_date,
            min(case when dose_description_calc_d1 = 'First Dose'
                then all_vax.received_date end)
                over (partition by all_vax.pat_id)
        )
        as first_dose_date_calc
    from
        all_vax
        inner join dose_date_vis_type on dose_date_vis_type.pat_id = all_vax.pat_id
    where
        all_vax.dose_description_vis_type is null
),

/*Fill in second dose date if no dose connected to dose 2 visit type
using filled in first dose date*/
fill_second_dose_date as (
    select
        immune_id,
        pat_id,
        received_date,
        booster_dose_date,
        case
            when received_date > first_dose_date_calc + interval '7 days'
            and second_dose_date is null
                then 'Second Dose'
            else dose_description_calc_d1 end
        as dose_description_calc_d2,
        /*Update second dose date*/
        coalesce(
            second_dose_date,
            min(case when dose_description_calc_d2 = 'Second Dose'
                then received_date end)
                over (partition by pat_id)
        )
        as second_dose_date_calc
    from
        fill_first_dose_date
),

/*Finally fill in boosters*/
imputed_dose_desc as (
    select
        immune_id,
        pat_id,
        case
            when received_date > second_dose_date_calc + interval '7 days'
            and booster_dose_date is null
                then 'Booster Dose'
            else dose_description_calc_d2 end
        as dose_description_imputed
    from
        fill_second_dose_date
)

/*Join back to all_vax to id final first and second doses for internal doses*/
select
    all_vax.immune_id,
    all_vax.pat_id,
    all_vax.received_date,
    all_vax.administration_location,
    all_vax.manufacturer_name,
    all_vax.internal_admin_ind,
    coalesce(
        all_vax.dose_description_vis_type,
        imputed_dose_desc.dose_description_imputed)
    as dose_description,
    case when row_number() over (
        partition by all_vax.pat_id, dose_description
        order by all_vax.received_date
    ) = 1
        then 1
    end as primary_dose_admin_ind,
    case when all_vax.dose_description_vis_type is null
        and dose_description is not null
        then 1 else 0
    end as imputed_dose_description_ind,
    all_vax.order_id,
    all_vax.order_csn,
    all_vax.inpatient_administration_ind
from
    all_vax
    left join imputed_dose_desc
        on imputed_dose_desc.immune_id = all_vax.immune_id
