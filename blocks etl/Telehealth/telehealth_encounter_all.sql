with telehealth_visits as (--region
    select
        stg_telehealth_encounter_provider.visit_provider_seq_key,
        stg_telehealth_encounter_provider.visit_key,
        stg_telehealth_encounter_provider.prov_key,
        stg_telehealth_encounter_provider.provider_specialty,
        stg_telehealth_encounter_provider.cohort_logic
    from
        {{ ref('stg_telehealth_encounter_provider') }} as stg_telehealth_encounter_provider
        inner join {{ ref('encounter_all') }} as encounter_all
            on encounter_all.visit_key = stg_telehealth_encounter_provider.visit_key
    where
        year(encounter_all.encounter_date) >= 2019
        and encounter_all.visit_type_id != '3086' -- office visit w psychiatry
        and encounter_all.department_id not in (
            101012074, -- hunting park care cln, not validated
            101035002  -- employee benefits
        )
),

inperson_visits as (
    select
        encounter_all.visit_key
    from
        {{ref('encounter_all') }} as encounter_all
        inner join {{source('cdw', 'department') }} as department
            on department.dept_key = encounter_all.dept_key
        left join {{ref('stg_encounter_telehealth') }} as stg_encounter_telehealth
            on stg_encounter_telehealth.visit_key = encounter_all.visit_key
    where
        year(encounter_all.encounter_date) >= 2019
        and encounter_all.encounter_date < current_date -- do not need future visits for in-person
        and stg_encounter_telehealth.visit_key is null -- not a video visit
        and encounter_type_id in (50, 101) -- appointment, office visit
        and (
            encounter_all.primary_care_ind = 1
            or encounter_all.specialty_care_ind = 1
            or cancel_noshow_ind = 1
        )
        and encounter_all.visit_type_id != '2817' -- 'e visit'
        --exclude 'INTERPRETER DEPARTM*', not a real visit, always scheduled with another real visit     
        and encounter_all.department_id != 1015002
        and lower(encounter_all.department_name) not like '%vx%'
        and lower(encounter_all.department_name) not like '%vax%'
        and lower(encounter_all.department_name) not like '%vaccine%'
        and lower(department.specialty) not in (
            'clinical laboratory',
            'radiology'
        )
        and lower(encounter_all.visit_type) not in (
            'pfizer dose two',
            'covid vaccine dose one',
            'city covid vx dose 1',
            'moderna dose two',
            'patient covid vx dose 1',
            'covid vx dose 2',
            'covid rapid test in sedation',
            'covid testing in sedation'
        )
),

cohort as (--region for telehealth and in-peron outpatient visits
    select
        coalesce(telehealth_visits.visit_provider_seq_key, encounter_all.visit_key) as visit_provider_seq_key,
        encounter_all.visit_key,
        encounter_all.encounter_date,
        encounter_all.appointment_date,
        coalesce(telehealth_visits.prov_key, encounter_all.prov_key) as prov_key,
        coalesce(telehealth_visits.provider_specialty, department.specialty) as provider_specialty,
        nvl2(telehealth_visits.visit_key, 'telehealth', 'inperson-outpatient') as visit_modality,
        coalesce(telehealth_visits.cohort_logic, 'inperson-outpatient') as cohort_logic,
        encounter_all.pat_key
    from
        {{ref('encounter_all') }} as encounter_all
        inner join {{source('cdw', 'department') }} as department
            on department.dept_key = encounter_all.dept_key
        left join telehealth_visits
            on telehealth_visits.visit_key = encounter_all.visit_key
        left join inperson_visits
            on inperson_visits.visit_key = encounter_all.visit_key
    where
        year(encounter_all.encounter_date) >= 2019
        and encounter_all.department_id != 1015002
        -- interpreter department, not a real visit, always scheduled with another real visit
        and (
            telehealth_visits.visit_key is not null
            or inperson_visits.visit_key is not null
        )
),

reason as (--region visit reason and comment
    select
        cohort.visit_key,
        max(decode(visit_reason.seq_num, 1, master_reason_for_visit.rsn_nm)) as reason_1,
        max(decode(visit_reason.seq_num, 2, master_reason_for_visit.rsn_nm)) as reason_2,
        max(decode(visit_reason.seq_num, 1, visit_reason.rsn_cmt)) as comment_1,
        max(decode(visit_reason.seq_num, 2, visit_reason.rsn_cmt)) as comment_2,
        case when reason_2 is null then reason_1 else reason_1 || ' , ' || reason_2 end as visit_reason,
        case when comment_2 is null then comment_1 else comment_1 || ' , ' || comment_2 end as visit_reason_comment
    from
        cohort
        inner join {{source('cdw', 'visit_reason') }} as  visit_reason
            on visit_reason.visit_key = cohort.visit_key
        inner join {{source('cdw', 'master_reason_for_visit') }} as  master_reason_for_visit
            on master_reason_for_visit.rsn_key = visit_reason.rsn_key
    group by
        cohort.visit_key
),

arti_abx as ( --region base for ARTI Diagnois, Antibiotic medications
    select
        cohort.visit_key,
        max(
            case
                when medication_order_administration.therapeutic_class_id = 1001
                    and medication_order_administration.pharmacy_class_id not in (11, 12)
                    then 1
                else 0
                end
            ) as abx_order_ind
    from
        cohort
        inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
            on diagnosis_encounter_all.visit_key = cohort.visit_key
        inner join {{ ref('lookup_diagnosis_icd10') }} as lookup_diagnosis_icd10
            on lookup_diagnosis_icd10.icd10_code = diagnosis_encounter_all.icd10_code
        left join {{ ref('medication_order_administration') }} as medication_order_administration
            on medication_order_administration.visit_key = cohort.visit_key
    where
        (-- arti diagnosis list
            lower(lookup_diagnosis_icd10.category) in ( -- general categories
                'j12',   -- viral pneumonia, not elsewhere classified
                'j20' -- acute bronchitis
            )
            or lower(lookup_diagnosis_icd10.icd10_code) in (-- specific influenza codes
                'j09.x2',
                -- influenza due to identified novel influenza a virus with other respiratory manifestations
                'j11.1' -- influenza due to unidentified influenza virus with other respiratory manifestations
                )
            or ( -- acute symptoms
                lower(lookup_diagnosis_icd10.category_desc) not like '%chronic%'
                and (
                lower(lookup_diagnosis_icd10.section) = 'j00-j06' -- acute upper respiratory infections
                or lower(lookup_diagnosis_icd10.category) in (
                    'a37',   -- whooping cough        
                    'j21', -- acute bronchiolitis
                    'j22', -- unspecified acute lower respiratory infection          
                    'r05', -- fever of unknown origin'
                    'r50' -- cough'
                    )
                )
            )
        )
    group by
        cohort.visit_key
),

followup as (-- region ed/inpatient admissions following video visit
    select
        cohort.visit_key,
        -- first dates
        min(
            case when encounter_all.ed_ind = 1 then encounter_all.hospital_admit_date end
        ) as first_ed_date,
        min(
            case when encounter_all.inpatient_ind = 1 then encounter_all.hospital_admit_date end
        ) as first_inpatient_date,
        min(
            case when lower(department.specialty) = 'urgent care' then encounter_all.hospital_admit_date end
        ) as first_urgent_care_date,
        min(
            case
                when encounter_all.primary_care_ind + encounter_all.specialty_care_ind = 1
                    then encounter_all.encounter_date end
        ) as first_outpatient_date
    from
        cohort
        inner join {{ ref('encounter_all') }} as encounter_all
            on encounter_all.pat_key = cohort.pat_key
        inner join {{source('cdw', 'department') }} as department
            on department.dept_key = encounter_all.dept_key
        left join {{ ref('stg_encounter_telehealth') }} as stg_encounter_telehealth
            on stg_encounter_telehealth.visit_key = encounter_all.visit_key
    where
        encounter_all.cancel_noshow_ind = 0
        and coalesce(encounter_all.appointment_date, encounter_all.hospital_admit_date) > cohort.appointment_date
        and date(coalesce(
            encounter_all.appointment_date,
            encounter_all.hospital_admit_date)
            ) - cohort.encounter_date <= 14
        and stg_encounter_telehealth.visit_key is null -- not a telehealth visit
    group by
        cohort.visit_key
),

lab_image_48 as (-- region lab_image -- about 6% of billing is missing visit_key
    select
        cohort.visit_key
    from
        cohort
        inner join {{ ref('procedure_order_all') }} as procedure_order_all
            on procedure_order_all.pat_key = cohort.pat_key
    where
        procedure_order_all.encounter_date - cohort.encounter_date between 1 and 2
        and (
            lower(procedure_order_all.procedure_group_name) like '%lab%'
            or lower(procedure_order_all.procedure_group_name) like '%imag%'
        )
    group by
        cohort.visit_key
),

mychop_active as (
    select
        cohort.visit_key,
        max(
            case when patient_mychart_status_history.stat_info_edit_dt <= cohort.appointment_date then 1 else 0 end
        ) as mychop_active_on_encounter_ind,
        max(
            case
                when date(patient_mychart_status_history.stat_info_edit_dt) - cohort.encounter_date
                    between 0 and 30 then 1 else 0 end
        ) as mychop_30day_activation_ind
    from
        cohort
        inner join {{ source('cdw', 'patient_mychart_status_history') }} as patient_mychart_status_history
            on cohort.pat_key = patient_mychart_status_history.pat_key
        inner join {{ source('cdw', 'dim_patient_mychart_info_status') }} as dim_patient_mychart_info_status
            on patient_mychart_status_history.dim_pat_mychart_info_stat_key = dim_patient_mychart_info_status.dim_pat_mychart_info_stat_key --noqa: L016
    where
        dim_patient_mychart_info_status.pat_mychart_info_stat_id = 1 --'ACTIVATED'
    group by
        cohort.visit_key
)

select
    cohort.visit_provider_seq_key,
    cohort.visit_modality,
    --encounter_all
    encounter_all.csn,
    encounter_all.encounter_date,
    encounter_all.appointment_date,
    to_char(encounter_all.appointment_date, 'hh24:mi') as appointment_time,
    encounter_all.visit_type,
    encounter_all.visit_type_id,
    encounter_all.encounter_type,
    encounter_all.encounter_type_id,
    encounter_all.appointment_status,
    encounter_all.appointment_status_id,
    visit.appt_cancel_24hr_ind as cancel_24hr_ind,
    visit.appt_cancel_48hr_ind as cancel_48hr_ind,
    case when encounter_all.appointment_status_id = 4 then 1 else 0 end as no_show_ind,
    case
        when encounter_all.cancel_noshow_ind = 0 and encounter_all.encounter_date < current_date then 1
        else 0
        end as completed_visit_ind,
    -- inclusion
    cohort.cohort_logic,
    case
        when cohort_logic = 'inpatient consults' then 'inpatient video consults'
        when department.specialty is null or  lower(department.specialty) = 'other' then 'other'
        when lower(encounter_all.department_name) like '%urg care%'
            or lower(encounter_all.department_name) like '%urgent care%' then 'urgent care'
        when lower(encounter_all.department_name) like '%care ntwk'
            or lower(encounter_all.department_name) like '%care network' then 'primary care'
        else 'specialty care'
        end as care_setting,
    --provider
    provider.prov_id as provider_id,
    initcap(provider.full_nm) as provider_name,
    cohort.provider_specialty,
    --department
    encounter_all.department_id,
    encounter_all.department_name,
    location.rpt_grp_6 as revenue_location_group,
    department.specialty,
    master_geography.zip as department_zip,
    master_geography.city as department_city,
    master_geography.county as department_county,
    master_geography.state as department_state,
    --payor
    encounter_all.payor_group,
    encounter_all.payor_name,
    --age at the time of encounter
    encounter_all.age_days,
    encounter_all.age_years,
    case
        when encounter_all.age_days < 42 then 'Less than 6 weeks'
        when encounter_all.age_days < 92 then '6 weeks to 3 months'
        when encounter_all.age_years < 1 then '3 to 12 months'
        when encounter_all.age_years < 2 then '12 to 24 months'
        else floor(encounter_all.age_years) || ' years'
        end as age_range,
    case
        when encounter_all.age_years < 2
            then floor(months_between(encounter_all.encounter_date, encounter_all.dob)) || ' month(s)'
        else floor(encounter_all.age_years) || ' years'
        end as age_months_years,
    -- reason
    reason.reason_1,
    reason.reason_2,
    reason.comment_1,
    reason.comment_2,
    reason.visit_reason,
    reason.visit_reason_comment,
    -- patient demographics 
    stg_patient.patient_name,
    stg_patient.dob,
    stg_patient.sex,
    stg_patient.mrn,
    stg_patient.mailing_city,
    stg_patient.mailing_state,
    stg_patient.mailing_zip,
    stg_patient.county,
    stg_patient.international_ind,
    stg_patient.race_ethnicity,
    stg_patient.preferred_language,
    upper(primary_provider.full_nm) as primary_care_provider_name,
    upper(primary_practice.prov_practice_nm) as primary_care_practice_name,
    -- primary diagnosis
    diagnosis_encounter_all.diagnosis_name as primary_diagnosis_name,
    -- antibiotics
    case when arti_abx.visit_key is not null then 1 else 0 end as abx_order_ind,
    -- follow-up            
    case
        when followup.first_ed_date - cohort.appointment_date <= '48 hours'::interval then 1 else 0 --noqa:L048
    end as ed_visit_48hr_ind,
    case
        when date(followup.first_ed_date) - cohort.encounter_date <= 7 then 1 else 0
    end as ed_visit_7day_ind,
    case
        when date(followup.first_inpatient_date) - cohort.encounter_date <= 7 then 1 else 0
    end as inpatient_visit_7day_ind,
    case
        when date(followup.first_inpatient_date) - cohort.encounter_date <= 14 then 1 else 0
    end as inpatient_visit_14day_ind,
    case
        when followup.first_urgent_care_date - cohort.appointment_date <= '48 hours'::interval --noqa:L048
            then 1 else 0
    end as urgent_care_visit_48hr_ind,
    case
        when date(followup.first_urgent_care_date) - cohort.encounter_date <= 7 then 1 else 0
    end as urgent_care_visit_7day_ind,
    case
        when followup.first_outpatient_date - cohort.encounter_date <= 14 then 1 else 0
    end as outpatient_14day_ind,
    case when lab_image_48.visit_key is not null then 1 else 0 end as lab_image_48hrs_ind,
    coalesce(mychop_active.mychop_active_on_encounter_ind, 0) as mychop_active_on_encounter_ind,
    case
        when cancel_noshow_ind = 1 or mychop_active.mychop_active_on_encounter_ind = 1 then null
        else coalesce(mychop_active.mychop_30day_activation_ind, 0)
        end as mychop_30day_activation_ind,
    cohort.prov_key,
    encounter_all.pat_key,
    cohort.visit_key
from
    cohort
    inner join {{ ref('encounter_all') }} on encounter_all.visit_key = cohort.visit_key
    inner join {{source('cdw','visit') }} as visit
        on visit.visit_key = encounter_all.visit_key
    inner join {{source('cdw','provider') }} as provider
        on provider.prov_key = cohort.prov_key
    inner join {{ ref('stg_patient') }}
        on stg_patient.pat_key = encounter_all.pat_key
    inner join {{source('cdw','patient') }} as patient
        on patient.pat_key = encounter_all.pat_key
    left join  {{ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        on diagnosis_encounter_all.visit_key = cohort.visit_key and diagnosis_encounter_all.visit_primary_ind = 1
    -- department attributes
    left join  {{ source('cdw', 'department') }} as department
        on department.dept_key = encounter_all.dept_key
    left join  {{ source('cdw', 'master_geography') }} as master_geography
        on master_geography.zip = substr(department.zip, 1, 5)
    left join  {{ source('cdw', 'location') }} as location --noqa: L029
        on location.loc_key = department.rev_loc_key
    -- primary care
    left join {{ source('cdw', 'provider') }} as primary_provider
        on patient.prov_key = primary_provider.prov_key
    left join {{ source('cdw', 'dim_provider_practice') }} as primary_practice
        on primary_provider.dim_prov_practice_key = primary_practice.dim_prov_practice_key
    -- cte joins
    left join reason on reason.visit_key = cohort.visit_key
    left join arti_abx on arti_abx.visit_key = cohort.visit_key
    left join followup on followup.visit_key = cohort.visit_key
    left join lab_image_48 on lab_image_48.visit_key = cohort.visit_key
    left join mychop_active on mychop_active.visit_key = cohort.visit_key
