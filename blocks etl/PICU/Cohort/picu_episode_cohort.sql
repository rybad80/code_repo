with cohort as (
    --region
    select
        'PHL' as picu_unit,
        {{
            dbt_utils.surrogate_key([
                'picu_unit',
                'vps_phl_admissions.caseid'
            ])
        }} as vps_episode_key,
        vps_phl_admissions.accountnum as csn,
        vps_phl_admissions.hospadmndate,
        vps_phl_admissions.hospadmtime,
        vps_phl_discharge_donation.hospdischargedatetime,
        vps_phl_admissions.caseid as case_id,
        vps_phl_admissions.icuadmdatetime,
        vps_phl_discharge_donation.icuphysicaldcdatetime,
        vps_phl_discharge_donation.icumedicaldcdatetime,
        vps_phl_admissions.race,
        vps_phl_admissions.ethnicity,
        vps_phl_admissions.patientorigin,
        case when vps_phl_referrals_info.referring_hosp = 'CHOP KOP' then 'Transferred from KOPH' end
            as inter_hospital,
        vps_phl_admissions.patienttype,
        vps_phl_admissions.postop as post_op,
        vps_phl_admissions.trauma,
        vps_phl_discharge_donation.hospitaloutcome as hospital_mortality_status,
        vps_phl_discharge_donation.hospitaldisposition,
        vps_phl_discharge_donation.mortality as picu_mortality_status,
        vps_phl_discharge_donation.disposition,
        vps_phl_pim3.pim3score as pim3_score,
        vps_phl_pim3.pim3rom as pim3_rom,
        vps_phl_prism3.pod as prism3_pod,
        vps_phl_prism3.score as prism3_score,
        vps_phl_prism3.predicted_los as prism3_predicted_los
    from
        {{source('vps_phl_ods', 'vps_phl_admissions')}} as vps_phl_admissions
        left join {{source('vps_phl_ods', 'vps_phl_discharge_donation')}} as vps_phl_discharge_donation
            on vps_phl_admissions.caseid = vps_phl_discharge_donation.caseid
        left join {{source('vps_phl_ods', 'vps_phl_pim3')}} as vps_phl_pim3
            on vps_phl_admissions.caseid = vps_phl_pim3.caseid
        left join {{source('vps_phl_ods', 'vps_phl_prism3')}} as vps_phl_prism3
            on vps_phl_admissions.caseid = vps_phl_prism3.caseid
        left join {{source('vps_phl_ods', 'vps_phl_referrals_info')}} as vps_phl_referrals_info
            on vps_phl_admissions.caseid = vps_phl_referrals_info.caseid
            and vps_phl_referrals_info.referring_hosp = 'CHOP KOP'
    where
        vps_phl_admissions.caseid != 34899

    union all

    select
        'KOPH' as picu_unit,
        {{
            dbt_utils.surrogate_key([
                'picu_unit',
                'vps_koph_admissions.caseid'
            ])
        }} as vps_episode_key,
        vps_koph_admissions.accountnum as csn,
        vps_koph_admissions.hospadmndate,
        vps_koph_admissions.hospadmtime,
        vps_koph_discharge_donation.hospdischargedatetime,
        vps_koph_admissions.caseid as case_id,
        vps_koph_admissions.icuadmdatetime,
        vps_koph_discharge_donation.icuphysicaldcdatetime,
        vps_koph_discharge_donation.icumedicaldcdatetime,
        vps_koph_admissions.race,
        vps_koph_admissions.ethnicity,
        vps_koph_admissions.patientorigin,
        case when vps_koph_referrals_info.referring_hosp = 'CHOP PHL' then 'Transferred from PHL' end
            as inter_hospital,
        vps_koph_admissions.patienttype,
        vps_koph_admissions.postop as post_op,
        vps_koph_admissions.trauma,
        vps_koph_discharge_donation.hospitaloutcome as hospital_mortality_status,
        vps_koph_discharge_donation.hospitaldisposition,
        vps_koph_discharge_donation.mortality as picu_mortality_status,
        vps_koph_discharge_donation.disposition,
        vps_koph_pim3.pim3score as pim3_score,
        vps_koph_pim3.pim3rom as pim3_rom,
        vps_koph_prism3.pod as prism3_pod,
        vps_koph_prism3.score as prism3_score,
        vps_koph_prism3.predicted_los as prism3_predicted_los
    from
        {{source('vps_koph_ods', 'vps_koph_admissions')}} as vps_koph_admissions
        left join {{source('vps_koph_ods', 'vps_koph_discharge_donation')}} as vps_koph_discharge_donation
            on vps_koph_admissions.caseid = vps_koph_discharge_donation.caseid
        left join {{source('vps_koph_ods', 'vps_koph_pim3')}} as vps_koph_pim3
            on vps_koph_admissions.caseid = vps_koph_pim3.caseid
        left join {{source('vps_koph_ods', 'vps_koph_prism3')}} as vps_koph_prism3
            on vps_koph_admissions.caseid = vps_koph_prism3.caseid
        left join {{source('vps_koph_ods', 'vps_koph_referrals_info')}} as vps_koph_referrals_info
            on vps_koph_admissions.caseid = vps_koph_referrals_info.caseid
            and vps_koph_referrals_info.referring_hosp = 'CHOP PHL'
    --end region
),

picu_episode_cohort as (
    --region
    select
        picu_unit,
        vps_episode_key,
        csn,
        case
            when hospadmtime is null then to_char(to_date(hospadmndate, 'mm/dd/yyyy'), 'yyyy-mm-dd') || ' 00:00:00'
            else to_date(hospadmndate, 'mm/dd/yyyy') || ' ' || cast(hospadmtime as time)
        end as hospital_admit_date,
        case
            when
                hospdischargedatetime is not null
                then to_timestamp(hospdischargedatetime, 'mm/dd/yyyy HH24:MI:SS')
        end as hospital_discharge_date,
        case_id,
        to_timestamp(icuadmdatetime, 'mm/dd/yyyy HH24:MI') as picu_admit_date,
        case
            when icuphysicaldcdatetime is not null then to_timestamp(icuphysicaldcdatetime, 'mm/dd/yyyy HH24:MI')
        end as picu_physical_discharge_date,
        case
            when icumedicaldcdatetime is not null then to_timestamp(icumedicaldcdatetime, 'mm/dd/yyyy HH24:MI')
        end as picu_medical_discharge_date,
        extract(
            epoch from picu_physical_discharge_date - picu_admit_date
        ) / 86400.0 as physical_los_days,
        extract(
            epoch from picu_medical_discharge_date - picu_admit_date
        ) / 86400.0 as medical_los_days,
        case
            when race = 'Unspecified' or race is null then 'Other/Mixed'
            else race
        end as race,
        ethnicity,
        case
            when lower(patientorigin) like 'another hospital%emergency%' then 'Outside Hospital ED'
            when lower(patientorigin) like 'another hospital%icu%' then 'Outside Hospital ICU'
            when lower(patientorigin) like 'another hospital%' then 'Outside Hospital Care Unit'
            when lower(patientorigin) in (
                'another icu in this hospital', 'another icu in this hospital (except nicu)',
                'another pediatric icu  (except nicu) - (in this hospital)',
                'another specialty pediatric icu (except nicu) in this hospital - other (specify in comments)'
            ) then 'CICU'
            when lower(patientorigin) in (
                'inpatient procedure suite', 'radiology/interventional radiology'
            ) then 'Radiology/Interventional Radiology'
            when lower(patientorigin) in (
                'inpatient procedure suite/procedure room (not cath lab and not radiology/interventional radiology)',
                'inpatient procedure suite (not cath lab)'
            ) then 'Inpatient Procedure Suite (GI Suite, MRI Suite)'
            when lower(patientorigin) in (
                'outpatient procedure suite', 'outpatient surgical center'
            ) then 'Outpatient Surgical Center'
            when lower(patientorigin) in (
                'dedicated technology dependent unit (transitional/progressive care unit)',
                'step-down unit/intermediate care unit'
            ) then 'PCU'
            when lower(patientorigin) = 'emergency department' then 'ED'
            when lower(patientorigin) in (
                'general care floor', 'telemetry unit'
            ) then 'General Floor'
            when lower(patientorigin) in (
                'nicu', 'nicu (in this hospital)'
            ) then 'NICU'
            when lower(patientorigin) in (
                'operating room', 'operating room (direct to icu)'
            ) then 'OR'
            when lower(patientorigin) = 'recovery room (pacu)' then 'PACU'
            when (
                patientorigin is null
                or lower(patientorigin) in (
                    'other',
                    'pulmonary rehab center',
                    'psychiatric/substance abuse/chemical dependence rehab center'
                )
            ) then 'Other (Psychiatric Care, Correctional Facility)'
            else patientorigin
        end as origin,
        inter_hospital,
        case
            when patienttype = 'Scheduled (> or = 12 Hours in Advance)' then 'Scheduled'
            when patienttype is null then 'Unknown'
            else patienttype
        end as schedule_type,
        post_op,
        trauma,
        hospital_mortality_status,
        case
            when lower(hospitaldisposition) like 'another hospital%icu%' then 'Outside Hospital ICU'
            when lower(hospitaldisposition) like 'another hospital%' then 'Outside Hospital Care Unit'
            when lower(hospitaldisposition) in ('home', 'hospice') then 'Home'
            when lower(hospitaldisposition) in (
                'medical examiner',
                'morgue / funeral / cremation professionals'
            ) then 'Morgue/Funeral/Cremation Professionals/Medical Examiner'
            when lower(hospitaldisposition) in (
                'other',
                'pulmonary rehab center',
                'psychiatric/substance abuse/chemical dependence rehab center'
            ) then 'Other (Psychiatric Care, Correctional Facility)'
        else hospitaldisposition
        end as hospital_disposition,
        picu_mortality_status,
        case
            when lower(disposition) like 'another hospital%icu%' then 'Outside Hospital ICU'
            when lower(disposition) like 'another hospital%' then 'Outside Hospital Care Unit'
            when lower(disposition) in (
                'another icu in current hospital',
                'another icu in this hospital (except nicu)',
                'another pediatric icu (except nicu) - (in this hospital)',
                'another specialty pediatric icu (except nicu) in this hospital - other (specify in comments)'
            ) then 'CICU'
            when lower(disposition) in (
                'dedicated technology dependent unit (transitional/progressive care unit)',
                'step-down unit/intermediate care unit'
            ) then 'PCU'
            when lower(disposition) in ('general care floor', 'telemetry unit') then 'General Floor'
            when lower(disposition) in ('home', 'hospice') then 'Home'
            when
                lower(disposition) = 'inpatient procedure suite (not cath lab)'
                then 'Inpatient Procedure Suite (GI Suite, MRI Suite)'
            when (
                    lower(disposition) in (
                        'medical examiner',
                        'morgue / funeral / cremation professionals'
                    ) or (
                        lower(disposition) = 'other' and picu_mortality_status = 'Died'
                    )
            ) then 'Morgue/Funeral/Cremation Professionals/Medical Examiner/Other'
            when lower(disposition) = 'nicu (in this hospital)' then 'NICU'
            when lower(disposition) = 'operating room' then 'OR'
            when lower(disposition) in (
                'transitional care/skilled nursing facility/chronic care facility',
                'transitional care/skilled nursing facility/chronic care facility (reimbursable by medicare and other insurance)'
            ) then 'Transitional Care/Skilled Nursing Facility/Chronic Care Facility'
            when lower(disposition) in (
                'pulmonary rehab center',
                'psychiatric/substance abuse/chemical dependence rehab center',
                'psychiatric/substance abuse/chemical dependence rehab center (in or out of facility)',
                'other'
            ) then 'Psychiatric/Substance Abuse/Chemical Dependence/Pulmonary Rehab Center/Other'
            else disposition
        end as picu_disposition,
        pim3_score,
        pim3_rom,
        prism3_pod,
        prism3_score,
        prism3_predicted_los
    from
        cohort
    --end region
),

adt_data as (
    --region
    select
        picu_episode_cohort.*,
        stg_patient.pat_key,
        adt_department.visit_key,
        stg_patient.mrn,
        stg_patient.patient_name,
        stg_patient.dob,
        extract(
            epoch from picu_episode_cohort.picu_admit_date - stg_patient.dob
        ) / 3600 / 24 / 365.25 as age_at_picu_admission_years,
        case
            when stg_patient.sex = 'F' then 'Female'
            when stg_patient.sex = 'M' then 'Male'
            when stg_patient.sex = 'U' then 'Unknown'
        end as legal_sex,
        adt_department.visit_event_key,
        adt_department.enter_date as adt_admit_date,
        adt_department.department_name as adt_admit_department,
        row_number() over (
            partition by adt_department.visit_key, picu_episode_cohort.picu_unit, picu_episode_cohort.case_id
            order by abs(extract(epoch from picu_episode_cohort.picu_admit_date - adt_department.enter_date) / 60)
        ) as admit_order
    from
        picu_episode_cohort
        left join {{ ref('adt_department') }} as adt_department
            on  picu_episode_cohort.csn = adt_department.csn
            and lower(adt_department.department_group_name) in ('picu', 'picu ovf' ,'mht', 'pcu')
        left join {{ ref('stg_patient') }} as stg_patient
            on adt_department.pat_key = stg_patient.pat_key
    --end region
)

select
    visit_event_key,
    pat_key,
    visit_key,
    picu_unit,
    vps_episode_key,
    mrn,
    patient_name,
    dob,
    csn,
    case_id,
    hospital_admit_date,
    hospital_discharge_date,
    picu_admit_date,
    adt_admit_department,
    picu_physical_discharge_date,
    picu_medical_discharge_date,
    age_at_picu_admission_years,
    physical_los_days,
    medical_los_days,
    legal_sex,
    cast(race as varchar(50)) as race,
    cast(ethnicity as varchar(50)) as ethnicity,
    cast(origin as varchar(60)) as origin,
    inter_hospital,
    cast(schedule_type as varchar(50)) as schedule_type,
    post_op,
    trauma,
    cast(hospital_mortality_status as varchar(10)) as hospital_mortality_status,
    cast(hospital_disposition as varchar(75)) as hospital_disposition,
    cast(picu_mortality_status as varchar(10)) as picu_mortality_status,
    cast(picu_disposition as varchar(100)) as picu_disposition,
    pim3_score,
    pim3_rom,
    prism3_pod,
    prism3_score,
    prism3_predicted_los
from
    adt_data
where
    admit_order = 1
