{{ config(meta = {
    'critical': true
}) }}

/*Isolation unit stays from treatment team starting 11/20/2020*/
with iso_unit_stay as (
    select
        encounter_inpatient.visit_key,
        provider_encounter_care_team.provider_care_team_start_date as enter_date,
        provider_encounter_care_team.provider_care_team_end_date as exit_date,
        provider_encounter_care_team.provider_care_team_group_name as unit_name
    from
        {{ref('encounter_inpatient')}} as encounter_inpatient
        inner join {{ref('provider_encounter_care_team')}} as provider_encounter_care_team
            on provider_encounter_care_team.visit_key = encounter_inpatient.visit_key
    where
        provider_encounter_care_team.provider_care_team_group_name in ('siu', 'pstu')
        and (
            (provider_encounter_care_team.source_summary = 'provider_record_ser'
            and provider_encounter_care_team.provider_care_team_start_date between '2020-11-20' and '2021-3-22')
        or provider_encounter_care_team.source_summary = 'provider_care_team_pct'
        )
),
/*Historical isolation unit stays prior to 11/20*/
iso_unit_stay_historical as (
    select
        adt_bed.visit_key,
        adt_bed.enter_date,
        adt_bed.exit_date,
        outbreak_master_pstu_siu_beds.room_group as unit_name
    from
        {{ref('adt_bed')}} as adt_bed
        inner join {{ref('outbreak_master_pstu_siu_beds')}}
            as outbreak_master_pstu_siu_beds
            on outbreak_master_pstu_siu_beds.room_name = adt_bed.room_name
     where
        adt_bed.enter_date between outbreak_master_pstu_siu_beds.start_date
        and coalesce(outbreak_master_pstu_siu_beds.end_date, current_date)
        and adt_bed.enter_date < '2020-11-20'
),

/*Combine pre/post 11/20 data*/
iso_unit_stay_combined as (
    select distinct
        coalesce(iso_unit_stay.visit_key, iso_unit_stay_historical.visit_key)
        as visit_key,
        case when iso_unit_stay.visit_key is null
            or iso_unit_stay_historical.visit_key is null
                then coalesce(iso_unit_stay.enter_date,
                iso_unit_stay_historical.enter_date)
            else min(iso_unit_stay.enter_date,
            iso_unit_stay_historical.enter_date)
        end as enter_date,
        case when iso_unit_stay.visit_key is null
            or iso_unit_stay_historical.visit_key is null
                then coalesce(iso_unit_stay.exit_date,
                iso_unit_stay_historical.exit_date)
            else max(iso_unit_stay.exit_date,
            iso_unit_stay_historical.exit_date)
        end as exit_date,
        coalesce(iso_unit_stay.unit_name, iso_unit_stay_historical.unit_name)
        as unit_name
    from
        iso_unit_stay_historical
        full join iso_unit_stay
            on iso_unit_stay.visit_key = iso_unit_stay_historical.visit_key
            and iso_unit_stay.unit_name = iso_unit_stay_historical.unit_name
            and iso_unit_stay.enter_date < iso_unit_stay_historical.exit_date
)

/*Resolve cases where siu and pstu treatment teams overlap*/
select
    iso_unit_stay_combined.visit_key,
    iso_unit_stay_combined.enter_date,
    /*truncate iso unit stay at start of next stay*/
    case when coalesce(iso_unit_stay_combined.exit_date, current_date)
    > lead(iso_unit_stay_combined.enter_date) over (
        partition by iso_unit_stay_combined.visit_key
        order by iso_unit_stay_combined.enter_date)
        then lead(iso_unit_stay_combined.enter_date) over (
            partition by iso_unit_stay_combined.visit_key
            order by iso_unit_stay_combined.enter_date)
        else iso_unit_stay_combined.exit_date
    end as exit_date,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    iso_unit_stay_combined.unit_name,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from
    iso_unit_stay_combined
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = iso_unit_stay_combined.visit_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
