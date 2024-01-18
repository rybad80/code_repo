/*
Author: Rob Olsen
Last Updated: 4/21/2021

Description: ECMO data from PC4 registry. Granularity is one row per ECMO run.
For the most part, if cannulas are completely removed and then
patient goes back on - that counts as a new run. But if cannulas are left in
and patients are reconnected to the circuit, it is left as one run.
*/

with ecmo_after_arrest as (

/*flag ECMO runs that started within two hours of a cardiac arrest*/

    select distinct
        registry_pc4_mechanical_support.r_mech_supp_id
    from
        {{source('cdw', 'registry_pc4_mechanical_support')}} as registry_pc4_mechanical_support
        inner join {{source('cdw', 'registry_pc4_cardiac_arrest')}} as registry_pc4_cardiac_arrest
            on registry_pc4_mechanical_support.r_enc_key = registry_pc4_cardiac_arrest.r_enc_key
            and round((
                    extract(epoch from registry_pc4_mechanical_support.r_mech_supp_init_dt
                        - registry_pc4_cardiac_arrest.r_card_arrest_strt_dt) / 60.0 / 60.0), 2
                ) between 0 and 2
)

    select
        cardiac_unit_encounter.pat_key,
        cardiac_unit_encounter.patient_name,
        cardiac_unit_encounter.mrn,
        cardiac_unit_encounter.dob,
        cardiac_unit_encounter.sex,
        cardiac_unit_encounter.hsp_vst_key,
        cardiac_unit_encounter.visit_key,
        registry_pc4_mechanical_support.r_enc_key as enc_key,
        registry_pc4_mechanical_support.r_mech_supp_id as mech_support_id,
        cardiac_unit_encounter.department_name,
        registry_pc4_mechanical_support.r_mech_supp_init_dt as ecmo_start_date,
        registry_pc4_mechanical_support.r_mech_supp_disc_dt as ecmo_end_date,

        /*ecmo_hours and ecmo_days are true counts of hours/days from cannulation to decannulation.
            ecmo_days_full counts a full day for each day patient was on ecmo. so for an ecmo run
            from 1/1 12:00 PM to 1/2 12:00 PM, ecmo_hours = 24, ecmo_days = 1, and ecmo_days_full = 2.
            need to add the +1 to the ecmo_days_full calculation to account for the full day of decannulation.*/

        round((
            extract(epoch from registry_pc4_mechanical_support.r_mech_supp_disc_dt
                - registry_pc4_mechanical_support.r_mech_supp_init_dt) / 60.0 / 60.0), 2
        ) as ecmo_hours,
        round((
            extract(epoch from registry_pc4_mechanical_support.r_mech_supp_disc_dt
                - registry_pc4_mechanical_support.r_mech_supp_init_dt) / 60.0 / 60.0 / 24.0), 2
        ) as ecmo_days,
        date(registry_pc4_mechanical_support.r_mech_supp_disc_dt)
                - date(registry_pc4_mechanical_support.r_mech_supp_init_dt) + 1
        as ecmo_days_full,
        case
            when ecmo_after_arrest.r_mech_supp_id is not null then 1 else 0 end
        as ecmo_within_two_hours_arrest_ind,
        registry_pc4_mechanical_support.mech_supp_cpr_ind as cpr_at_cannulation_ind,
        registry_pc4_mechanical_support.mech_supp_prev_cpr_ind as cpr_two_hours_prior_cannulation_ind,
        case when registry_pc4_mechanical_support.mech_supp_cpr_ind = 1 then 'CPR at cannulation'
            when registry_pc4_mechanical_support.mech_supp_prev_cpr_ind = 1
                then 'CPR within two hours prior to cannulation'
            else 'No CPR' end as cpr_cannulation_description,
        registry_pc4_mechanical_support.mech_supp_cicu_strt_ind as ecmo_cicu_start_ind,
        registry_pc4_mechanical_support.mech_supp_cicu_end_ind as ecmo_cicu_end_ind,
        row_number() over (
            partition by registry_pc4_mechanical_support.r_enc_key
            order by registry_pc4_mechanical_support.r_mech_supp_init_dt
        ) as ecmo_seq,
        count(
            distinct registry_pc4_mechanical_support.r_mech_supp_id) over(
            partition by registry_pc4_mechanical_support.r_enc_key
        ) as encounter_total_ecmo_runs

    from
        {{source('cdw', 'registry_pc4_mechanical_support')}} as registry_pc4_mechanical_support
    left join ecmo_after_arrest
        on registry_pc4_mechanical_support.r_mech_supp_id = ecmo_after_arrest.r_mech_supp_id
    inner join {{ref('cardiac_unit_encounter')}} as cardiac_unit_encounter
        on registry_pc4_mechanical_support.r_enc_key = cardiac_unit_encounter.enc_key

    where
        lower(registry_pc4_mechanical_support.mech_supp_type_desc) = 'ecmo'
        and registry_pc4_mechanical_support.cur_rec_ind = 1
        and lower(cardiac_unit_encounter.registry) = 'pc4'
