{{ config(meta = {
    'critical': true
}) }}

/*Registry history for CHOP patient vaccine reg
and Philly School District (PSD)/non-affiliated
healthcare worker (HCW) registry*/
with registry_history as (
    select
        reg_data_hx_membership.record_id,
        reg_data_hx_membership.contact_date,
        reg_data_hx_membership.line,
        reg_data_hx_membership.registry_id,
        case when reg_data_hx_membership.status_c = 1
            then 1 else 0
        end as registry_start_ind,
        stg_patient.pat_id,
        stg_patient.dob
    from
        {{source('clarity_ods', 'reg_data_hx_membership')}} as reg_data_hx_membership
        inner join {{source('clarity_ods', 'registry_data_info')}} as registry_data_info
            on registry_data_info.record_id = reg_data_hx_membership.record_id
        /*Join to stg_patient to remove test pats and get demo
        info for young adult exclusions*/
        inner join {{ref('stg_patient')}} as stg_patient
            on stg_patient.pat_id = registry_data_info.networked_id
    where
        reg_data_hx_membership.registry_id in (
            100408, --eligible CHOP pat registry
            100415, --external CHOP pat registry
            100411) --PSD/HCW upload registry
),

/*Intervals on vax elig registry*/
ya_registry_history as (
    select
        record_id,
        contact_date as registry_start_dt,
        lead(contact_date) over (
            partition by record_id
            order by contact_date,
                line
        ) as registry_end_dt,
        registry_start_ind,
        pat_id,
        dob
    from
        registry_history
    where
        registry_id = 100408
),

/*Patients */
ya_patients as (
    select distinct
        pat_id
    from
        ya_registry_history
    where
        /*Registry start record*/
        registry_start_ind = 1
        /*On registry on 2/23...*/
        and ('2021-2-23' between registry_start_dt
            and coalesce(registry_end_dt, current_date)
        /*or any time after*/
            or registry_start_dt >= '2021-2-23')
        /*24 or younger at start of 2021*/
        and extract(epoch from '2021-1-1' - dob) / 60.0 / 60.0 / 24.0 / 365.25 --noqa: PRS
        <= 24
),

registry_patients as (
    select
        registry_history.record_id,
        registry_history.pat_id,
        max(case when ya_patients.pat_id is not null
            then 1 else 0 end)
        as ya_vax_registry_ind,
        max(case when registry_history.registry_id = 100415
            then 1 else 0 end)
        as ya_external_registry_ind,
        max(case when registry_history.registry_id = 100411
            then 1 else 0 end)
        as roster_registry_ind
    from
        registry_history
        left join ya_patients on ya_patients.pat_id = registry_history.pat_id
    where
        /*Make sure one of the indicators = 1 so we don't
        include >24 y/o pats on the eligibility reg that were excluded
        in the last step*/
        registry_history.registry_id in (100415, 100411)
        or ya_patients.pat_id is not null
    group by
        registry_history.record_id,
        registry_history.pat_id
),

/*Get patient source detail for roster patients*/
/*This is also used to identify patients who were uploaded as a community clinic vaccination*/
registry_metrics as (
    select
        registry_data_info.networked_id as pat_id,
        max(case when reg_data_metrics.metrics_id = 1393214
            and reg_data_metrics.metric_string_value = 1
            then 1 else 0 end)
        as comm_clinic_patient_ind,
        max(case when reg_data_metrics.metrics_id = 1387860
            then regexp_replace(zc_roster_identifier.name, '^COVID-19 VACCINE ', '') end)
        as patient_type
    from
        {{source('clarity_ods', 'reg_data_metrics')}} as reg_data_metrics
        /*get pat_id from registry_data_info*/
        inner join {{source('clarity_ods', 'registry_data_info')}} as registry_data_info
            on registry_data_info.record_id = reg_data_metrics.record_id
        left join {{source('clarity_ods', 'zc_roster_identifier')}} as zc_roster_identifier
            on cast(zc_roster_identifier.roster_identifier_c as varchar(18))
            = reg_data_metrics.metric_string_value
    where
        reg_data_metrics.metrics_id in (
            1387860, -- roster ID
            1393214) -- is record on community clinic roster?
    group by
        registry_data_info.networked_id
),

/*Identify individuals who have scheduled at clinics
so they can be added to the right group*/
vax_visits as (
    select
        stg_encounter.pat_id,
        max(case
                when lookup_covid_vaccine_clinic_type.patient_population = 'School Personnel'
                and stg_encounter.cancel_noshow_ind = 0
            then 1 else 0 end)
        as school_ind,
        max(case
                when lookup_covid_vaccine_clinic_type.patient_population = 'Non-affiliated Healthcare Worker'
                and stg_encounter.cancel_noshow_ind = 0
            then 1 else 0 end)
        as hcw_ind,
        max(case
                when lookup_covid_vaccine_clinic_type.patient_population = 'Community Clinic Vaccine Recipient'
                and stg_encounter.cancel_noshow_ind = 0
            then 1 else 0 end)
        as comm_clinic_ind,
        max(case when stg_encounter.cancel_noshow_ind = 0 then 1 else 0 end)
        as non_cancelled_appt_ind
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{ref('lookup_covid_vaccine_visit_types')}}
            as lookup_covid_vaccine_visit_types
            on lookup_covid_vaccine_visit_types.visit_type_id
            = stg_encounter.visit_type_id
        left join {{ref('lookup_covid_vaccine_clinic_type')}}
            as lookup_covid_vaccine_clinic_type
            on lookup_covid_vaccine_clinic_type.department_id
            = stg_encounter.department_id
            and stg_encounter.encounter_date
                between lookup_covid_vaccine_clinic_type.align_start_date
                and coalesce(lookup_covid_vaccine_clinic_type.align_end_date, current_date + interval '1 year')
    group by
        stg_encounter.pat_id
),

scheduling_topic as (
    select
        pat_id,
        /*follow_up_dttm in UTC*/
        min(
            to_timestamp(timezone(follow_up_dttm, 'UTC', 'America/New_York'),
            'YYYY-MM-DD HH:MI:SS')
        ) as follow_up_dttm
    from
        {{source('clarity_ods', 'follow_up_topics')}}
    where
        /*covid scheduling topic*/
        follow_up_topic_c = 1000020
    group by
        pat_id
),

vax_admin as (
    select
        pat_id,
        max(
            case when comm_clin_abstracted_ind = 1
            or clinic_type = 'Community Clinic Vaccine Recipient'
            then 1 else 0 end)
        as comm_clinic_dose_ind,
        max(case when clinic_type = 'CHOP Patient'
            then 1 else 0 end)
        as patient_dose_ind,
        max(case when clinic_type = 'School Personnel'
            then 1 else 0 end)
        as school_dose_ind,
        max(case when clinic_type = 'Non-affiliated Healthcare Worker'
            then 1 else 0 end)
        as hcw_dose_ind
    from
        {{ref('stg_outbreak_covid_vaccination_admin')}}
    group by
        pat_id
),

all_patients as (
    select
        pat_id
    from
        registry_patients

    union

    select
        pat_id
    from
        vax_visits

    union

    select
        pat_id
    from
        scheduling_topic

    union

    select
        pat_id
    from
        vax_admin
)

select
    all_patients.pat_id,
    case
        /*Classify patients who received a vaccine*/
        when vax_admin.patient_dose_ind = 1
            then 'CHOP Patient'
        when vax_admin.school_dose_ind = 1
            then 'School Personnel'
        when vax_admin.hcw_dose_ind = 1
            then 'Non-affiliated Healthcare Worker'
        when vax_admin.comm_clinic_dose_ind = 1
            then 'Community Clinic Vaccine Recipient'
        when registry_metrics.comm_clinic_patient_ind = 1
            then 'Community Clinic Vaccine Recipient'
        /*Classify patients who scheduled*/
        when vax_visits.school_ind = 1
            then 'School Personnel'
        when vax_visits.hcw_ind = 1
            then 'Non-affiliated Healthcare Worker'
        when vax_visits.comm_clinic_ind = 1
            then 'Community Clinic Vaccine Recipient'
        when vax_visits.school_ind = 0
            and vax_visits.hcw_ind = 0
            and vax_visits.comm_clinic_ind = 0
            and vax_visits.non_cancelled_appt_ind = 1
            then 'CHOP Patient'
        /*Classify unscheduled patients*/
        when registry_metrics.patient_type in (
                'PRE K SCHOOL',
                'VIRTUAL ACCESS CENTER',
                'DAY CARE CENTER',
                'ARCHDIOCESE OF PHILADELPHIA',
                'INDEPENDENT SCHOOL',
                'CHARTER SCHOOL',
                'PHILADELPHIA SCHOOL DISTRICT')
                then 'School Personnel'
        when registry_patients.ya_vax_registry_ind = 1
            or registry_patients.ya_external_registry_ind = 1
            then 'CHOP Patient'
        when registry_metrics.patient_type in (
                'CAREGIVERS',
                'COMM HEALTHCARE AFFILIATES')
                then 'Non-affiliated Healthcare Worker'
        /*Classify as patient if not on some other registry*/
        when registry_patients.pat_id is null
            then 'CHOP Patient'
    end as patient_population,
    case
        when patient_population = 'School Personnel'
            and registry_metrics.patient_type in (
                'PRE K SCHOOL',
                'VIRTUAL ACCESS CENTER',
                'DAY CARE CENTER',
                'ARCHDIOCESE OF PHILADELPHIA',
                'INDEPENDENT SCHOOL',
                'CHARTER SCHOOL',
                'PHILADELPHIA SCHOOL DISTRICT')
            then registry_metrics.patient_type
        when patient_population = 'CHOP Patient'
            and registry_patients.ya_vax_registry_ind = 1
            then 'YOUNG ADULT PATIENT - VACCINE REGISTRY'
        when patient_population = 'CHOP Patient'
            and registry_patients.ya_external_registry_ind = 1
            then 'YOUNG ADULT PATIENT - EXTERNAL SIGN-UP'
        when patient_population = 'CHOP Patient'
            and scheduling_topic.pat_id is not null
            then 'OTHER INVITED CHOP PATIENT'
        when patient_population = 'Non-affiliated Healthcare Worker'
            and registry_metrics.patient_type in (
                'CAREGIVERS',
                'COMM HEALTHCARE AFFILIATES')
            then registry_metrics.patient_type
    end as patient_type,
    scheduling_topic.follow_up_dttm
from
    all_patients
    left join registry_patients on registry_patients.pat_id = all_patients.pat_id
    left join registry_metrics on registry_metrics.pat_id = all_patients.pat_id
    left join vax_visits on vax_visits.pat_id = all_patients.pat_id
    left join scheduling_topic on scheduling_topic.pat_id = all_patients.pat_id
    left join vax_admin on vax_admin.pat_id = all_patients.pat_id
