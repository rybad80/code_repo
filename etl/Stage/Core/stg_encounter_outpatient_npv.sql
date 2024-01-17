{{ config(
	materialized='table',
	dist='visit_key',
	meta={
		'critical': true
	}
) }}

with previous_encounter_all as (
    --region identify all previous completed/arrived visits for the patient within a given specialty
    select
        stg_encounter_outpatient_raw.visit_key,
        stg_encounter_outpatient_raw.encounter_date,
        past_encounter.visit_key as last_completed_visit_key,
        past_encounter.encounter_date as last_completed_encounter_date,
        row_number() over(
            partition by
                stg_encounter_outpatient_raw.visit_key
            order by
                past_encounter.appointment_date_no_blank desc,
                past_encounter.visit_key
        ) as visit_num
    from
        {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
        inner join {{ref('stg_encounter_outpatient_raw')}} as past_encounter
            on stg_encounter_outpatient_raw.pat_key = past_encounter.pat_key
    where
        -- appointment statuses: completed/arrived/na
        past_encounter.appointment_status_id in ('2', '6', '-2')
        -- keeps only appointments occuring before current record in last 3 years
        and past_encounter.appointment_date_no_blank < stg_encounter_outpatient_raw.appointment_date_no_blank
        and past_encounter.appointment_date_no_blank
            > stg_encounter_outpatient_raw.appointment_date_no_blank - 1097
        and stg_encounter_outpatient_raw.specialty_name = past_encounter.specialty_name
    --end region
)

select
    stg_encounter_outpatient_raw.visit_key,
    stg_encounter_outpatient_raw.encounter_key,
    previous_encounter_all.last_completed_visit_key,
    previous_encounter_all.last_completed_encounter_date,
    stg_encounter_payor.payor_key,
    stg_encounter_payor.payor_name,
    stg_encounter_payor.payor_group,
    stg_encounter_outpatient_raw.prov_key,
    provider.prov_id as provider_id,
    initcap(provider.full_nm) as provider_name,
    provider.prov_type as provider_type,
    count(distinct stg_encounter_outpatient_raw.specialty_name) over (
        partition by stg_encounter_outpatient_raw.pat_key, stg_encounter_outpatient_raw.encounter_date
    ) as n_specialty,
    case
        when extract(epoch from previous_encounter_all.encounter_date --noqa: L028
            - previous_encounter_all.last_completed_encounter_date) / (365.25) >= 3
            then 1
        when previous_encounter_all.last_completed_encounter_date is null
        then 1
        else 0
    end as new_patient_3yr_ind,
    case
        when (--provider type definition
            lower(provider.prov_type) = 'physician' --physician
            or lower(provider.prov_type) in
                ('midwife', 'nurse practitioner',
                'nurse anesthetist', 'clinical nurse specialist',
                'physician assistant') -- APP
            or lower(provider.prov_type) like '%psycholog%' --pyschologist
            )
            and (stg_office_visit_grouper.physician_app_psych_visit_ind = 1
        or stg_encounter_outpatient_raw.primary_care_ind = 1)
        then 1
        /*
        * Licensed social workers for Behavioral Health are included in this grouper
        */
        when
            lower(stg_encounter_outpatient_raw.specialty_name) = 'behavioral health services'
            and lower(provider.prov_type) = 'social worker'
            and provider.title = 'LCSW'
            and stg_office_visit_grouper.physician_app_psych_visit_ind = 1
        then 1
        else 0
    end as physician_app_psych_visit_ind,
    stg_encounter_outpatient_raw.online_scheduled_ind,
    case
        when (stg_encounter_outpatient_raw.intended_use_id = '1009' -- specialty care
            -- excluding ancillary services from phys_app_psych_visit inclusion
            and lower(stg_encounter_outpatient_raw.specialty_name) not in (
                'physical therapy', 'speech', 'audiology',
                'occupational therapy', 'clinical nutrition')
            and physician_app_psych_visit_ind = 1
            and stg_encounter_outpatient_raw.online_scheduled_ind = 1)
        then 1 else 0
    end as phys_app_psych_online_scheduled_ind,
    case
        when
            stg_encounter_outpatient_raw.appointment_made_date <= stg_encounter_outpatient_raw.encounter_date
            and coalesce(new_patient_3yr_ind, 1) = 1 --new to specialty 3 year ind
            and stg_encounter_outpatient_raw.international_ind = 0
            -- including all payor groups except non-participatory insurances provided by Lindsay Hagan
            and lookup_new_patient_visit_lag_payors.payor_incl_ind = 1
            and lower(stg_encounter_outpatient_raw.chop_market) in ('primary', 'secondary')
            --physician/app/psychologist provider
            and (lower(provider.prov_type) = 'physician' --physician
                    or lower(provider.prov_type) in
                    ('midwife', 'nurse practitioner',
                    'nurse anesthetist', 'clinical nurse specialist',
                    'physician assistant') -- APP
                    or lower(provider.prov_type) like '%psycholog%') --pyschologist
            and (( --Oupatient Specialty Care
                stg_encounter_outpatient_raw.intended_use_id = '1009'
                and lower(stg_encounter_outpatient_raw.visit_type) not like '%research%'
                --removing Kuhn, Kira NP from ID to exclude our pre-transplant evaluations specialty clinic
                and provider.prov_id not in ('2000140')
                -- including all departments and visit types unless requested to remove by stakeholders
                and lookup_office_visit_npv_consolidated_grouper.npv_included_ind = 1
                -- including only patients with < 3 visits in the same specialty in a given encounter
                and n_specialty < 3
                --physician/app/psychologist visit
                and stg_office_visit_grouper.physician_app_psych_visit_ind = 1
                )
                or ( --Primary Care Network
                stg_encounter_outpatient_raw.intended_use_id = '1013'
                and stg_encounter_outpatient_raw.age_years >= 1
                ))
        then date(stg_encounter_outpatient_raw.encounter_date)
            - date(stg_encounter_outpatient_raw.appointment_made_date) --noqa: PRS
        else null
        /**same calculation as appointment_lag_days, only populated for
        qualifying npv and completed/scheduled/arrived (else NULL)**/
    end as npv_appointment_lag_days
from
    {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
    left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
        on stg_encounter_payor.visit_key = stg_encounter_outpatient_raw.visit_key
    left join
        {{ref('lookup_office_visit_npv_consolidated_grouper')}} as lookup_office_visit_npv_consolidated_grouper
        on stg_encounter_outpatient_raw.department_id = lookup_office_visit_npv_consolidated_grouper.department_id
            and stg_encounter_outpatient_raw.visit_type_id
            = lookup_office_visit_npv_consolidated_grouper.visit_type_id
            and lookup_office_visit_npv_consolidated_grouper.npv_included_ind = 1
    left join {{ref('lookup_new_patient_visit_lag_payors')}} as lookup_new_patient_visit_lag_payors
        on stg_encounter_payor.payor_id = lookup_new_patient_visit_lag_payors.payor_id
            and lookup_new_patient_visit_lag_payors.payor_incl_ind = 1
	left join {{ref('stg_office_visit_grouper')}} as stg_office_visit_grouper
		on stg_encounter_outpatient_raw.visit_type_id = stg_office_visit_grouper.visit_type_id
        and stg_encounter_outpatient_raw.department_id = stg_office_visit_grouper.department_id
    left join previous_encounter_all
        on previous_encounter_all.visit_key = stg_encounter_outpatient_raw.visit_key
        and previous_encounter_all.visit_num = 1
    inner join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = stg_encounter_outpatient_raw.prov_key
