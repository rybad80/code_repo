{{ config(meta = {
    'critical': true
}) }}

with triage_encounters as (
select
    stg_encounter.encounter_key,
    stg_encounter.visit_key,
    stg_encounter.csn,
    stg_encounter.patient_key,
    stg_encounter.pat_key,
    stg_encounter.mrn,
    stg_encounter.pat_id,
    stg_encounter.encounter_date,
    date_trunc('month', stg_encounter.encounter_date)::date as encounter_month_year,
    add_months(date_trunc('month', stg_encounter.encounter_date), 1)::date as encounter_month_year_next,
    stg_encounter.encounter_instant,
    (hour(stg_encounter.encounter_instant) * 100)
    + ((minute(stg_encounter.encounter_instant) / 15) * 15) as enc_hhmm_period_start,
    stg_encounter.department_id,
    stg_encounter.department_name,
    pc_dept.department_display_name,
    case
        when stg_encounter.department_id = 89296012
            then 'Care Network, CHOP Campus'
        else pc_dept.site_display_name
    end as site_name,
    dim_provider.provider_key,
    stg_encounter.provider_id,
    stg_encounter.provider_name,
    dim_provider.provider_type,
    stg_encounter.patient_name,
    stg_encounter.sex,
    round(stg_encounter.age_years, 2) as encounter_age,
    case
        when stg_encounter.age_months < 6 then 'Infant (under 6 months)'
        when stg_encounter.age_years < 3 then 'Infant (6 months-2.99 years)'
        when stg_encounter.age_years >= 19 then 'Adult (19+ years)'
        when stg_encounter.age_years >= 12 then 'Adolescent (12-18.99 yrs)'
        else 'Child (3-11.99 yrs)'
    end as encounter_age_category,
    stg_encounter.encounter_closed_ind,
    stg_encounter.encounter_close_date,
    case
        when
            stg_encounter.enc_closed_user_id is null
            then 'Encounter not closed' else prov_close_user.full_name
    end as close_provider_name,
    case
        when
            stg_encounter.enc_closed_user_id is null
            then 'Encounter not closed' else prov_close_user.provider_type
    end as close_provider_type,
    prov_close_user.provider_key as close_provider_key
from {{ ref('stg_encounter') }} as stg_encounter
    inner join
        {{ ref('lookup_care_network_department_cost_center_sites') }} as pc_dept
        on stg_encounter.department_id = pc_dept.department_id
    left join
        {{ ref('stg_erroneous_encounters_nurse_triage') }} as erroneous_encounters
        on stg_encounter.encounter_key = erroneous_encounters.encounter_key
    inner join {{ ref('dim_provider') }} as dim_provider
        on stg_encounter.provider_key = dim_provider.provider_key
    left join {{ ref('dim_provider') }} as prov_close_user
        on stg_encounter.enc_closed_user_id = prov_close_user.user_id
    -- This is just to de-duplicate Dr. Khoi Dang (user_id = DANGK) who has 
    -- both Surgical Resident (Inactive) and Physician (Active) records.
    -- Going to add a seq_num to stg_provider to join to most recent record
    -- so as not to exclude providers who are no longer with CHOP.
            and prov_close_user.active_stat_ind = 1
where stg_encounter.encounter_date between '2018-07-01' and (current_date - 1)
    and stg_encounter.encounter_type_id = 71 -- 'Nurse Triage'
    and erroneous_encounters.visit_key is null
group by
    stg_encounter.encounter_key,
    stg_encounter.visit_key,
    stg_encounter.csn,
    stg_encounter.patient_key,
    stg_encounter.pat_key,
    stg_encounter.mrn,
    stg_encounter.pat_id,
    stg_encounter.encounter_date,
    stg_encounter.encounter_instant,
    enc_hhmm_period_start,
    stg_encounter.department_id,
    stg_encounter.department_name,
    pc_dept.department_display_name,
    pc_dept.site_display_name,
    dim_provider.provider_key,
    stg_encounter.provider_id,
    stg_encounter.provider_name,
    dim_provider.provider_type,
    stg_encounter.patient_name,
    stg_encounter.sex,
    encounter_age,
    encounter_age_category,
    stg_encounter.encounter_closed_ind,
    stg_encounter.encounter_close_date,
    stg_encounter.enc_closed_user_id,
    close_provider_name,
    close_provider_type,
    prov_close_user.provider_key
)

select
    triage_encounters.encounter_key,
    triage_encounters.visit_key,
    triage_encounters.csn,
    triage_encounters.patient_key,
    triage_encounters.pat_key,
    triage_encounters.pat_id,
    triage_encounters.mrn,
    triage_encounters.encounter_date,
    triage_encounters.encounter_instant,
    triage_encounters.enc_hhmm_period_start,
    triage_encounters.department_id,
    triage_encounters.department_name,
    triage_encounters.department_display_name,
    triage_encounters.site_name,
    stg_nurse_triage_call_contract.ahp_contract_dept_id,
    stg_nurse_triage_call_contract.ahp_contract_dept_name,
    stg_nurse_triage_call_contract.ahp_contract_dept_display,
    case
        -- North Hills Care Network (now inactive)
        when triage_encounters.department_id = 66315012
            or triage_encounters.department_id != 37
            -- only after hours encounters contracted to a PC department
            or stg_nurse_triage_call_contract.primary_care_triage_ind = 1
        then 1 else 0
    end as primary_care_triage_ind,
    coalesce(stg_nurse_triage_call_contract.external_encounter_ind, 0) as external_encounter_ind,
    triage_encounters.provider_key,
    triage_encounters.provider_id,
    triage_encounters.provider_name,
    triage_encounters.provider_type,
    triage_encounters.patient_name,
    triage_encounters.sex,
    coalesce(
        stg_patient_ods.preferred_language,
        'None specified'
    ) as preferred_language,
    case
        when stg_patient_ods.preferred_language is null then 'None specified'
        when stg_patient_ods.preferred_language = 'ENGLISH' then 'English'
        else 'Non-English'
    end as language_group,
    case
        when lower(patient.intrptr_needed_yn) = 'y'
        then 1
        when  lower(patient.intrptr_needed_yn) = 'n'
        then 0
        else -2
    end  as interpreter_needed_ind,
    stg_patient_ods.race_ethnicity,
    stg_patient_ods.dob::date as dob,
    stg_patient_ods.deceased_ind,
    stg_patient_ods.texting_opt_in_ind,
    triage_encounters.encounter_age,
    triage_encounters.encounter_age_category,
    coalesce(
        pc_active_pats.department_name,
        pc_active_pats_next.department_name,
        'No active PC dept'
    ) as pc_active_department_name,
    coalesce(
        pc_active_pats.pcp_name,
        pc_active_pats_next.pcp_name,
        'No active PC prov'
    ) as pc_active_provider_name,
    triage_encounters.encounter_closed_ind,
    triage_encounters.encounter_close_date,
    triage_encounters.close_provider_key,
    triage_encounters.close_provider_name,
    triage_encounters.close_provider_type
from triage_encounters
    inner join {{ ref('stg_patient_ods') }} as stg_patient_ods
        on triage_encounters.patient_key = stg_patient_ods.patient_key
    inner join {{ source('clarity_ods', 'patient') }} as patient
        on triage_encounters.pat_id = patient.pat_id
    left join
        {{ ref('care_network_primary_care_active_patients') }} as pc_active_pats
        on triage_encounters.pat_key = pc_active_pats.pat_key
            and pc_active_pats.month_year = triage_encounters.encounter_month_year
    left join
        {{ ref('care_network_primary_care_active_patients') }}
            as pc_active_pats_next
        on triage_encounters.pat_key = pc_active_pats_next.pat_key
            and pc_active_pats_next.month_year = triage_encounters.encounter_month_year_next
    left join {{ ref('stg_nurse_triage_call_contract') }} as stg_nurse_triage_call_contract
        on stg_nurse_triage_call_contract.encounter_key = triage_encounters.encounter_key
group by
    triage_encounters.encounter_key,
    triage_encounters.visit_key,
    triage_encounters.csn,
    triage_encounters.patient_key,
    triage_encounters.pat_key,
    triage_encounters.pat_id,
    triage_encounters.mrn,
    triage_encounters.encounter_date,
    triage_encounters.encounter_instant,
    enc_hhmm_period_start,
    triage_encounters.department_id,
    triage_encounters.department_name,
    triage_encounters.department_display_name,
    triage_encounters.site_name,
    stg_nurse_triage_call_contract.ahp_contract_dept_id,
    stg_nurse_triage_call_contract.ahp_contract_dept_name,
    stg_nurse_triage_call_contract.ahp_contract_dept_display,
    stg_nurse_triage_call_contract.primary_care_triage_ind,
    stg_nurse_triage_call_contract.external_encounter_ind,
    triage_encounters.provider_key,
    triage_encounters.provider_id,
    triage_encounters.provider_name,
    triage_encounters.provider_type,
    triage_encounters.patient_name,
    triage_encounters.sex,
    stg_patient_ods.preferred_language,
    language_group,
    interpreter_needed_ind,
    stg_patient_ods.race_ethnicity,
    stg_patient_ods.dob,
    stg_patient_ods.deceased_ind,
    stg_patient_ods.texting_opt_in_ind,
    encounter_age,
    encounter_age_category,
    pc_active_department_name,
    pc_active_provider_name,
    triage_encounters.encounter_closed_ind,
    triage_encounters.encounter_close_date,
    triage_encounters.close_provider_key,
    close_provider_name,
    close_provider_type
