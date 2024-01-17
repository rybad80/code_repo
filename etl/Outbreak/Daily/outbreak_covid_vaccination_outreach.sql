{{ config(meta = {
    'critical': true
}) }}

/*Fields for invitation logic*/
with scheduling_order as (
    select
        stg_outbreak_covid_vaccination_cohort.pat_id,
        min(procedure_order_clinical.placed_date) as placed_date
    from
        {{ref('stg_outbreak_covid_vaccination_cohort')}}
        as stg_outbreak_covid_vaccination_cohort
        inner join {{ref('procedure_order_clinical')}} as procedure_order_clinical
            on procedure_order_clinical.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
        inner join {{source('clarity_ods', 'grouper_compiled_rec_list')}}
            as grouper_compiled_rec_list
            on grouper_compiled_rec_list.grouper_records_numeric_id
            = procedure_order_clinical.procedure_id
    where
        /*scheduling order grouper*/
        grouper_compiled_rec_list.base_grouper_id = '118272'
    group by
        stg_outbreak_covid_vaccination_cohort.pat_id
),

mychop_email_invite as (
    select
        stg_outbreak_covid_vaccination_cohort.pat_id,
        min(pat_enc_letters.ltr_status_chg_tm) as activation_email_date
    from
        {{ref('stg_outbreak_covid_vaccination_cohort')}}
        as stg_outbreak_covid_vaccination_cohort
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
        inner join {{source('clarity_ods', 'pat_enc_letters')}} as pat_enc_letters
            on pat_enc_letters.pat_enc_csn_id = stg_encounter.csn
    where
        stg_encounter.encounter_type_id = 105 --'Letter (out)'
        and pat_enc_letters.letter_template_id = '30249' --imm invite letter
        and pat_enc_letters.ltr_status_c = 3 -- Sent
    group by
        stg_outbreak_covid_vaccination_cohort.pat_id
),

mychop_activation as (
    select
        stg_outbreak_covid_vaccination_cohort.pat_id,
        min(pat_myc_stat_hx.myc_stat_hx_tmstp) as mychop_activation_date
    from
        {{ref('stg_outbreak_covid_vaccination_cohort')}}
        as stg_outbreak_covid_vaccination_cohort
        inner join {{source('clarity_ods', 'pat_myc_stat_hx')}} as pat_myc_stat_hx
            on pat_myc_stat_hx.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
    where
        pat_myc_stat_hx.myc_stat_hx_c = 1 --Activated
    group by
        stg_outbreak_covid_vaccination_cohort.pat_id
)

select
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_patient.dob,
    stg_outbreak_covid_vaccination_cohort.patient_population,
    stg_outbreak_covid_vaccination_cohort.patient_type,
    stg_patient.email_address,
    stg_patient.mailing_state,
    stg_patient.county,
    stg_patient.mailing_zip,
    stg_patient.race_ethnicity,
    stg_patient.preferred_language,
    patient_all.payor_group,
    case
        /*HCWs considered invited when they have sched order
        and activation email*/
        when stg_outbreak_covid_vaccination_cohort.patient_population = 'Non-affiliated Healthcare Worker'
            then max(scheduling_order.placed_date,
                mychop_email_invite.activation_email_date)
        /*Everyone else considered invited when they have either
        sched order or scheduling outreach topic*/
        else min(
            coalesce(scheduling_order.placed_date, stg_outbreak_covid_vaccination_cohort.follow_up_dttm),
            coalesce(stg_outbreak_covid_vaccination_cohort.follow_up_dttm, scheduling_order.placed_date))
    /*Earlier of scheduling order or scheduling topic*/
    end as scheduling_invitation_date,
    case
        when scheduling_invitation_date
        is not null
            then 1 else 0
    end as invited_ind,
    mychop_activation.mychop_activation_date,
    coalesce(patient_all.mychop_activation_ind, 0) as mychop_activation_ind,
    outbreak_covid_vaccination.dose_1_appointment_csn,
    outbreak_covid_vaccination.dose_1_appointment_date,
    coalesce(
        outbreak_covid_vaccination.dose_1_scheduled_ind, 0)
    as dose_1_scheduled_ind,
    outbreak_covid_vaccination.dose_1_appointment_location,
    outbreak_covid_vaccination.dose_1_cancel_noshow_ind,
    outbreak_covid_vaccination.dose_1_received_date,
    coalesce(
        outbreak_covid_vaccination.dose_1_received_ind, 0)
    as dose_1_received_ind,
    outbreak_covid_vaccination.dose_1_manufacturer_name,
    outbreak_covid_vaccination.dose_2_appointment_csn,
    outbreak_covid_vaccination.dose_2_appointment_date,
    coalesce(
        outbreak_covid_vaccination.dose_2_scheduled_ind, 0)
    as dose_2_scheduled_ind,
    outbreak_covid_vaccination.dose_2_appointment_location,
    outbreak_covid_vaccination.dose_2_cancel_noshow_ind,
    outbreak_covid_vaccination.dose_2_received_date,
    coalesce(
        outbreak_covid_vaccination.dose_2_received_ind, 0)
    as dose_2_received_ind,
    outbreak_covid_vaccination.dose_2_manufacturer_name,
    stg_patient.pat_key,
    stg_outbreak_covid_vaccination_cohort.pat_id
from
    {{ref('stg_outbreak_covid_vaccination_cohort')}} as stg_outbreak_covid_vaccination_cohort
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
    inner join {{ref('patient_all')}} as patient_all
        on patient_all.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
    left join {{ref('outbreak_covid_vaccination')}} as outbreak_covid_vaccination
        on outbreak_covid_vaccination.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
    left join scheduling_order
        on scheduling_order.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
    left join mychop_activation
        on mychop_activation.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
    left join mychop_email_invite
        on mychop_email_invite.pat_id = stg_outbreak_covid_vaccination_cohort.pat_id
where
    /*invited patients*/
    stg_outbreak_covid_vaccination_cohort.patient_type = 'OTHER INVITED CHOP PATIENT'
    /*part of pre-identified elig group*/
    or stg_outbreak_covid_vaccination_cohort.patient_population in (
        'School Personnel', 'Non-affiliated Healthcare Worker')
    or stg_outbreak_covid_vaccination_cohort.patient_type like 'YOUNG ADULT PATIENT%'
