{{ config(meta = {
    'critical': true
}) }}

with
transfer_details as (
    /* some patients have more than one record on this table, we'll use the latest only */
    select
        eoc_id,
        transferredtodataval,
        intrafacilitytransferdataval,
        hospital,
        other_hospital_specify,
        unitdataval,
        row_number() over(
            partition by eoc_id
            order by filledformid desc
        ) as rn
    from
        {{ source('neo_velos_ods', 'neo_velos_dischargetransferdetails') }}
)
select
    eoc.filledformid as eoc_id,
    neo_chnd_patient.chnd_pat_key,
    neo_chnd_patient.chnd_id,
    neo_chnd_patient.mrn,
    eoc.patient_account_num as har,
    cast(eoc.hospital_admission_date || ' ' || eoc.hospital_adm_time as datetime) as hospital_admit_date,
    cast(eoc.nicu_adm_date || ' ' || eoc.nicu_adm_time as datetime) as nicu_admit_date,
    cast(
        dischargefromnicu.nicu_discharge_date || ' ' || dischargefromnicu.nicu_discharge_time as datetime
    ) as nicu_discharge_date,
    eoc.day_of_life as admit_day_of_life,
    case
        /* using wildcards to work around typos in historical data, ie  `Hopsital` instead of `Hospital` */
        when admit.referral_sourcedispval like '% Cardiac Intensive Care Unit' then 'CHOP CICU'
        when admit.referral_sourcedispval like '% Clinic' then 'CHOP Clinic'
        when admit.referral_sourcedispval like '% ER' then 'CHOP ER'
        when admit.referral_sourcedispval like '% Labor and Delivery' then 'CHOP SDU'
        when admit.referral_sourcedispval like '% OR' then 'CHOP OR'
        when admit.referral_sourcedispval like '% PICU or Surgical ICU' then 'CHOP PICU'
        when admit.referral_sourcedispval like '% Ward%' then 'CHOP Non-ICU'
        when admit.referral_sourcedataval = 'Other Hospital' and admit.referralhospunitdataval = 'Other Hosp NICU'
            then 'Other Hospital NICU'
        when admit.referral_sourcedataval = 'Other Hospital' and admit.referralhospunitdataval != 'Other Hosp NICU'
            then 'Other Hospital non-NICU'
        when admit.referral_sourcedispval is null then 'Missing'
        else admit.referral_sourcedataval
    end as admit_referral_source,
    coalesce(admit.referral_hospital, admit.specify) as admit_referral_hospital,
    admit.nicu_admission_temperature as nicu_admit_temperature,
    coalesce(admit.primaryadmreasondataval, 'Missing') as nicu_primary_admit_reason,
    eoc.re_admissiondispval as nicu_readmission,
    eoc.re_admissionreasondispval as nicu_readmission_reason,
    dischargefromnicu.initialdischargedispdispval as nicu_discharge_disposition,
    dischargefromnicu.apneacardiorespmonitordispval as nicu_discharge_w_apnea_cardio_resp_monitor,
    dischargefromnicu.enteralfeedingsdisnicudispval as nicu_discharge_enteral_feedings,
    dischargefromnicu.instimedischargenicudispval as nicu_discharge_insurance,
    case
        when dischargefromnicu.nicu_discharge_date is null then 'Still Admitted'
        when dischargefromnicu.initialdischargedispdispval = 'Died' then 'Died'
        when home.eoc_id is not null then 'Home or Foster Care'
        when transfer_details.eoc_id is not null then 'Transfer - ' || transfer_details.transferredtodataval
        else 'Record Incomplete'
    end as nicu_discharge_type,
    coalesce(
        transfer_details.intrafacilitytransferdataval,
        coalesce(transfer_details.hospital, transfer_details.other_hospital_specify)
            || ' - ' || transfer_details.unitdataval
    ) as transfer_destination
from
    {{ ref('neo_chnd_patient' ) }} as neo_chnd_patient
    left join {{ source('neo_velos_ods', 'neo_velos_eoc') }} as eoc
        on eoc.patient_pk = neo_chnd_patient.chnd_pat_key
    left join {{ source('neo_velos_ods', 'neo_velos_transportandadmission') }} as admit
        on admit.eoc_id = eoc.filledformid
    left join {{ source('neo_velos_ods', 'neo_velos_dischargefromnicu') }} as dischargefromnicu
        on dischargefromnicu.eoc_id = eoc.filledformid
    left join transfer_details
        on transfer_details.eoc_id = eoc.filledformid
        and transfer_details.rn = 1
    left join {{ source('neo_velos_ods', 'neo_velos_homeorfostercare') }} as home
        on home.eoc_id = eoc.filledformid
where
    dischargefromnicu.eoc_id is not null
