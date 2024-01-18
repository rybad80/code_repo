with all_past_encounters as (
    select
        stg_encounter_ed.visit_key,
        stg_encounter_ed.pat_key,
        stg_encounter_ed.patient_name,
        stg_encounter_ed.mrn,
        stg_encounter_ed.dob,
        stg_encounter_ed.csn,
        stg_encounter_ed.encounter_date,
        stg_encounter_ed.age_years,
        stg_encounter_ed.department_name,
        null as department_id,
        stg_encounter_ed.prov_key
    from
      {{ref('stg_encounter_ed')}} as stg_encounter_ed
    where
      stg_encounter_ed.ed_patients_seen_ind = 1
    union
    select
      stg_past_encounters.visit_key,
      stg_past_encounters.pat_key,
      stg_past_encounters.patient_name,
      stg_past_encounters.mrn,
      stg_past_encounters.dob,
      stg_past_encounters.csn,
      stg_past_encounters.encounter_date,
      stg_past_encounters.age_years,
      stg_past_encounters.department_name,
      stg_past_encounters.department_id,
      stg_past_encounters.prov_key
    from {{ref('stg_past_encounters')}} as stg_past_encounters
),

past_encounters_row as (
    select
        stg_encounter_ed.pat_key,
        stg_encounter_ed.visit_key,
        stg_encounter_ed.encounter_date as ed_visit,
        all_past_encounters.encounter_date as encounter_date,
        all_past_encounters.department_name,
        row_number() over (partition by stg_encounter_ed.visit_key
            order by all_past_encounters.pat_key, all_past_encounters.encounter_date desc)
            as most_recent_visit_number
    from all_past_encounters
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = all_past_encounters.prov_key
        inner join {{ref('stg_encounter_ed')}} as stg_encounter_ed
            on all_past_encounters.pat_key = stg_encounter_ed.pat_key
    where
       /* FROM THE OUTBREAK BLOCK
        Main Clinical Lab Drive Thru Providers
        VOORHEES SPECIALTY CARE [  532764]
        BUCKS COUNTY SPECIALTY CARE [  532742]
        ROBERTS CENTER [  532741]
        BRANDYWINE VALLEY SPECIALTY CARE [  532760]
        Lab Provider, Site Three  [532743]
        WOODLAND REC CENTER    [532765]
        Lab Provider,Site Eight    [532799]
        Lab Provider,Site Seven    [532798]
        Drive Thru Providers, live 5/22
        Roberts Ctr Drive Up Test (532855)
        Bucks Co Drive Up Test  (532856)
        Brandywine Vly Drive Up Test (532857)
        Voorhees Drive Up Testing  (532858)
        Wood Rec Ctr Drive Up Test (532859)
        Mill Creek Rec Ctr Drive Up Test (532860)
        Kop Drive Up Testing   (532861)
        Wissinoming Prk Drive Up Test (532862)
        Pop Up Mobile Testing (532863)
        JJS COVID TESTING (532864)
        Phila Campus Covid Testing (532865)
    */
      provider.prov_id not in (
            '532760',
            '532742',
            '532741',
            '532764',
            '532743',
            '532765',
            '532799',
            '532798',
            '532855',
            '532856',
            '532857',
            '532858',
            '532859',
            '532860',
            '532861',
            '532862',
            '532863',
            '532864',
            '532865'
      )
      and provider.prov_id != '17388' -- Lab Provider, Clinical Main
      --only include visits prior to ed encounter
      and all_past_encounters.encounter_date < stg_encounter_ed.encounter_date
      and stg_encounter_ed.ed_patients_seen_ind = 0
),

visit_prior_to_ed as (
    select * from past_encounters_row where most_recent_visit_number = 1
)

select
    stg_encounter_ed.visit_key,
    stg_encounter_ed.patient_name,
    stg_encounter_ed.mrn,
    stg_encounter_ed.dob,
    stg_encounter_ed.csn,
    stg_encounter_ed.encounter_date,
    stg_encounter_ed.sex,
    stg_encounter_ed.age_years,
    stg_encounter_ed.age_days,
    stg_encounter_ed.ed_arrival_date,
    stg_encounter_ed.ed_triage_start_date,
    stg_encounter_ed.ed_roomed_date,
    stg_encounter_ed.md_evaluation_date,
    stg_encounter_ed.ed_discharge_date,
    stg_encounter_ed.edecu_admit_date,
    stg_encounter_ed.edecu_discharge_date,
    stg_encounter_ed.ed_los_hrs,
    stg_encounter_ed.edecu_los_hrs,
    stg_encounter_ed.initial_ed_department_center_id,
    stg_encounter_ed.initial_ed_department_center_abbr,
    stg_encounter_ed.final_ed_department_center_id,
    stg_encounter_ed.final_ed_department_center_abbr,
    stg_encounter_ed.clinical_dx_primary_icd10,
    stg_encounter_ed.clinical_dx_primary_icd9,
    stg_encounter_ed.clinical_dx_all_dx_nm,
    stg_encounter_ed.billing_dx_primary_icd9,
    stg_encounter_ed.billing_dx_primary_icd10,
    stg_encounter_ed.primary_reason_for_visit_name,
    stg_encounter_ed.primary_reason_for_visit_id,
    stg_encounter_ed.acuity_esi,
    stg_encounter_ed.inpatient_ind,
    stg_encounter_ed.inpatient_admit_date,
    stg_encounter_ed.admission_department_name,
    stg_encounter_ed.hospital_discharge_date,
    stg_encounter_ed.edecu_ind,
    stg_encounter_ed.icu_ind,
    stg_encounter_ed.revisit_72_hour_ind,
    stg_encounter_ed.patient_address_seq_num,
    stg_encounter_ed.patient_address_zip_code,
    stg_encounter_payor.payor_name,
    stg_encounter_payor.payor_group,
    stg_encounter_ed.primary_care_location,
    stg_encounter_ed.ed_visit_language,
    stg_encounter_ed.ed_visit_language_comment,
    stg_encounter_ed.complex_chronic_condition_ind,
    stg_encounter_ed.medically_complex_ind,
    stg_encounter_ed.billing_dx_primary_dx_key,
    stg_encounter_ed.clinical_dx_primary_dx_key,
    stg_encounter_ed.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    stg_encounter_ed.prov_key,
    stg_encounter_ed.department_name,
    stg_encounter_ed.department_id,
    case when visit_prior_to_ed.encounter_date is null then 1
        when extract(epoch from stg_encounter_ed.encounter_date
            - visit_prior_to_ed.encounter_date) / (365.25) >= 3 then 1
        else 0 end as unestablished_patient_to_ed_ind
from
    {{ref('stg_encounter_ed')}} as stg_encounter_ed
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter_ed.encounter_key
    left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter_payor.visit_key = stg_encounter_ed.visit_key
    left join
        visit_prior_to_ed
            on stg_encounter_ed.visit_key = visit_prior_to_ed.visit_key

where
  stg_encounter_ed.ed_patients_seen_ind = 0
