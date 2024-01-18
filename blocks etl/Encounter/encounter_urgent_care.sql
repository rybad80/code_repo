with cohort as (
	select
        stg_encounter_outpatient.encounter_key,
        stg_encounter_outpatient.patient_key,
        stg_encounter_outpatient.encounter_date,
        stg_encounter_outpatient.csn,
        zc_acuity_level.name as acuity,
        coalesce(zc_ed_disposition.name, 'NOT APPLICABLE') as disposition,
        case
            when pat_enc_hsp.ed_disposition_c in (5, 6, 7, 19)
            then 1
            else 0
        end as lwbs_ind
    from
        {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
        inner join {{source('clarity_ods','pat_enc_hsp')}} as pat_enc_hsp
            on pat_enc_hsp.pat_enc_csn_id = stg_encounter_outpatient.csn
        left join {{source('clarity_ods','zc_acuity_level')}} as zc_acuity_level
           on zc_acuity_level.acuity_level_c = pat_enc_hsp.acuity_level_c
        left join {{source('clarity_ods','zc_ed_disposition')}} as zc_ed_disposition
            on zc_ed_disposition.ed_disposition_c = pat_enc_hsp.ed_disposition_c
        where
            urgent_care_ind = 1
),

events as (
    select
        cohort.csn,
        max(case when event_type = '50' then event_time else null end) as ed_arrival_date,
        max(case when event_type = '55' then event_time else null end) as ed_roomed_date,
        max(case when event_type = '120' then event_time else null end) as ed_triage_start_date,
        max(case when event_type in ('110', '111') then event_time else null end) as md_eval_start_date,
        max(case when event_type = '95' then event_time else null end) as md_eval_end_date
    from
        cohort
    inner join {{source('clarity_ods','ed_iev_pat_info')}} as ed_iev_pat_info
        on ed_iev_pat_info.pat_enc_csn_id = cohort.csn
    inner join {{source('clarity_ods','ed_iev_event_info')}} as ed_iev_event_info
        on ed_iev_event_info.event_id = ed_iev_pat_info.event_id
    where
        ed_iev_event_info.event_type in ('50', '55', '95', '110', '111', '120')
    group by
        cohort.csn
),

pc_filter as (
    select
        cohort.csn,
        max(case when stg_encounter_outpatient.patient_key is not null then 1 else 0 end) as pc_patient_ind
    from
        cohort
        inner join {{ ref('stg_encounter_outpatient') }} as stg_encounter_outpatient
            on cohort.patient_key = stg_encounter_outpatient.patient_key
            and stg_encounter_outpatient.encounter_date <= cohort.encounter_date
            and stg_encounter_outpatient.primary_care_ind = 1
    group by
        cohort.csn
)

select
    stg_encounter_outpatient.encounter_key,
    stg_encounter_outpatient.patient_key,
    stg_encounter_outpatient.patient_name,
    stg_encounter_outpatient.mrn,
    stg_encounter_outpatient.dob,
    stg_encounter_outpatient.csn,
    stg_encounter_outpatient.encounter_date,
    stg_encounter_outpatient.hospital_discharge_date as ed_discharge_date,
    stg_encounter_outpatient.sex,
    stg_encounter_outpatient.age_years,
    stg_encounter_outpatient.age_days,
	case
        when stg_encounter_outpatient.age_years < 1
        then 'infancy (< 1 year)'
        when stg_encounter_outpatient.age_years >= 1 and stg_encounter_outpatient.age_years < 5
        then 'early childhood (>= 1 year & <5 years)'
        when stg_encounter_outpatient.age_years >= 5 and stg_encounter_outpatient.age_years < 13
        then 'late childhood (>= 5 years & < 13 years)'
        when stg_encounter_outpatient.age_years >= 13 and stg_encounter_outpatient.age_years < 18
        then 'adolescence (>= 13 years & < 18 years)'
        when stg_encounter_outpatient.age_years >= 18 and stg_encounter_outpatient.age_years < 30
        then 'adult (>= 18 years & < 30 years)'
        when stg_encounter_outpatient.age_years >= 30
        then 'adult (>= 30)'
    end as age_group,
    dim_provider.provider_key,
    stg_encounter_outpatient.provider_name,
    dim_provider.full_name as referring_provider_name,
    stg_encounter_outpatient.department_key,
    stg_encounter_outpatient.department_name,
    stg_encounter_outpatient.department_id,
    stg_encounter_outpatient.patient_address_seq_num,
    stg_encounter_outpatient.patient_address_zip_code,
    stg_encounter_outpatient.chop_market,
    stg_encounter_outpatient.region_category,
    stg_encounter_outpatient.payor_name,
    stg_encounter_outpatient.payor_group,
    cohort.acuity,
	case
        when cohort.disposition = 'Discharge'
        then 'Discharge'
        when cohort.disposition = 'NOT APPLICABLE' and events.ed_roomed_date is not null
        then 'Discharge'
        else cohort.disposition
    end as disposition,
    lwbs_ind,
    diagnosis_encounter_all.diagnosis_key,
    diagnosis_encounter_all.diagnosis_name as primary_diagnosis,
	ed_arrival_date,
	hour((ed_arrival_date)) as ed_arrival_hour,
    ed_roomed_date,
	ed_triage_start_date,
	md_eval_start_date,
	md_eval_end_date,
    ((extract(epoch from ed_roomed_date - ed_arrival_date) / 60.0)) as time_to_room_mins,
	((extract(epoch from ed_triage_start_date - ed_arrival_date) / 60.0)) as time_to_triage_mins,
	((extract(epoch from md_eval_start_date - ed_arrival_date) / 60.0)) as time_to_eval_mins,
	((extract(epoch from md_eval_end_date - md_eval_start_date) / 60.0)) as md_eval_length_mins,
	((extract(epoch from hospital_discharge_date - ed_arrival_date) / 60.0)) as ed_los,
	coalesce(pc_patient_ind, 0) as pc_patient_ind,
    stg_encounter_outpatient.complex_chronic_condition_ind,
    stg_encounter_outpatient.medically_complex_ind,
    stg_encounter_outpatient.tech_dependent_ind,
    stg_encounter_outpatient.international_ind
from
	cohort
    inner join {{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
        on stg_encounter_outpatient.encounter_key = cohort.encounter_key
	left join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
        on cohort.encounter_key = diagnosis_encounter_all.encounter_key
        and diagnosis_encounter_all.ed_primary_ind = 1
	left join {{source('clarity_ods','pat_enc')}} as pat_enc
        on cohort.csn = pat_enc.pat_enc_csn_id
	left join {{ref('dim_provider')}} as dim_provider
        on pat_enc.referral_source_id = dim_provider.prov_id
	left join pc_filter
        on cohort.csn = pc_filter.csn
    left join events
        on cohort.csn = events.csn
