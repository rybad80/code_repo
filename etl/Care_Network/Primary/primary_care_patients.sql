{{ config(meta = {
    'critical': true
}) }}

with transferred_out_pats as (
select
    patient_key,
    max(inactive_start_dt) as transfer_date
from {{ref('stg_care_network_patient_transfer')}}
--patients who are still inactive have end_dt = current_date
where inactive_end_dt = current_date
group by patient_key
),
cell_phone as (
select
    stg_primary_care_patients.patient_key,
    other_communctn.other_communic_num as mobile_phone,
    other_communctn.contact_priority,
    other_communctn.contact_notes,
    row_number() over(
        partition by other_communctn.pat_id order by other_communctn.contact_priority asc nulls last
    ) as row_num
from
    {{ref('stg_primary_care_patients')}} as stg_primary_care_patients
    inner join
        {{ source('clarity_ods', 'other_communctn') }} as other_communctn on
            stg_primary_care_patients.pat_id = other_communctn.pat_id
where
    other_communctn.other_communic_c = 1 --cell phone 
)
select
    stg_primary_care_patients.patient_key,
    stg_primary_care_patients.mrn,
    stg_primary_care_patients.pat_id,
    stg_patient_ods.patient_name,
    stg_patient_ods.preferred_name,
    stg_patient_ods.dob,
    stg_patient_ods.sex_assigned_at_birth,
    stg_patient_ods.sex,
    stg_patient_ods.gender_identity,
    stg_patient_ods.race_ethnicity,
    stg_patient_ods.preferred_language,
    stg_patient_ods.email_address,
    stg_patient_ods.mailing_zip,
    stg_patient_ods.home_phone,
    cell_phone.mobile_phone,
    stg_patient_ods.texting_opt_in_ind,
    stg_mychop_status.mychop_activation_ind,
    stg_mychop_status.mychop_declined_ind,
    stg_patient_ods.current_age,
    stg_patient_ods.current_age * 12.0 as current_age_months,
    first_enc.encounter_date as first_encounter_date,
    first_enc.age_years as first_encounter_age_years,
    last_enc.encounter_date as last_encounter_date,
    last_enc.age_years as last_encounter_age_years,
    last_enc.payor_group as last_pc_payor_group,
    last_enc.payor_name as last_pc_payor_name,
    pc_dept.department_id as last_department_id,
    pc_dept.department_display_name as last_department_name,
    pc_dept.site_display_name as last_site_name,
    last_enc.provider_name as last_provider,
    transferred_out_pats.transfer_date,
    case when last_encounter_date >= (current_date - cast('2 years' as interval))::date
        and transfer_date is null
        and stg_patient_ods.current_age < 22
        and stg_patient_ods.deceased_ind != 1
    then 1 else 0 end as current_active_ind,
    stg_patient_ods.deceased_ind,
    stg_patient_ods.current_record_ind,
    well_visits.last_well_encounter_key,
    well_visits.last_well_date,
    well_visits.last_well_age_years,
    round(months_between(current_date, last_well_date), 1) as months_since_last_well,
    well_visits.num_wells_by_fifteen,
    case when (current_age_months < 15
            and (num_wells_by_fifteen < 6
                or num_wells_by_fifteen is null)
            -- only include patients who have been active starting from 0-31 days of age
            or first_enc.age_days > 31) then null
        when num_wells_by_fifteen >= 6
    then 1 else 0 end as six_well_fifteen_ind,
    well_visits.num_wells_fifteen_thirty,
    case when (current_age_months < 30
            and (num_wells_fifteen_thirty < 2
                or num_wells_fifteen_thirty is null)
            -- exclude patients whose first encounter was at > 19 months old
            or first_enc.age_months > 19) then null
        when num_wells_fifteen_thirty >= 2
    then 1 else 0 end as two_well_fifteen_thirty_ind,
    case when (num_wells_by_fifteen + num_wells_fifteen_thirty) >= 8
            then 1
        when (current_age_months < 30
            and ((num_wells_by_fifteen + num_wells_fifteen_thirty) < 8
                or two_well_fifteen_thirty_ind is null)
                -- patient is > 3 months at their first PC encounter
            or first_enc.age_days > 31) then null
        else 0
    end as eight_well_thirty_ind,
    well_visits.next_well_encounter_key,
    well_visits.next_well_date,
    well_visits.next_well_age_years,
    case when current_active_ind = 1
        and ((current_age_months < 15 and six_well_fifteen_ind is null)
            or (current_age_months between 15 and 30 and two_well_fifteen_thirty_ind is null)
            or (current_age_months >= 36
                and stg_patient_ods.current_age < 19
                and (last_well_date < (current_date - cast('1 years' as interval))::date
                    or last_well_age_months < 33))
        ) then 1
        else 0
    end as well_visit_incomplete_ind
from {{ref('stg_primary_care_patients')}} as stg_primary_care_patients
    left join {{ref('stg_care_network_primary_care_patient_wells')}} as well_visits
        on stg_primary_care_patients.patient_key = well_visits.patient_key
    inner join {{ref('stg_patient_ods')}} as stg_patient_ods
        on stg_patient_ods.patient_key = stg_primary_care_patients.patient_key
    left join {{ref('stg_mychop_status')}} as stg_mychop_status
        on stg_mychop_status.patient_key = stg_primary_care_patients.patient_key
    left join transferred_out_pats
        on transferred_out_pats.patient_key = stg_primary_care_patients.patient_key
    inner join {{ref('stg_encounter_outpatient')}} as first_enc
        on stg_primary_care_patients.first_encounter_key = first_enc.encounter_key
    inner join {{ref('stg_encounter_outpatient')}} as last_enc
        on stg_primary_care_patients.last_encounter_key = last_enc.encounter_key
    inner join {{ref('lookup_care_network_department_cost_center_sites')}} as pc_dept
        on pc_dept.department_id = last_enc.department_id
    left join cell_phone
        on cell_phone.patient_key = stg_primary_care_patients.patient_key
            and cell_phone.row_num = 1
