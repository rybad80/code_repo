select
    stg_encounter.visit_key,
    call_communication_tracking.commn_trckng_key,
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.department_name,
    stg_encounter.encounter_type,
    stg_encounter.sex,
    stg_patient.race_ethnicity,
    stg_encounter.dob,
    stg_encounter.csn,
    call_communication_tracking.commn_id,
    stg_encounter.encounter_date,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    year(stg_encounter.encounter_date) as calendar_year,
    date_trunc('month', stg_encounter.encounter_date) as calendar_month,
    encounter_provider.prov_id as encounter_provider_id,
    encounter_employee.ad_login as encounter_provider_ad_login,
    initcap(encounter_provider.full_nm) as encounter_provider_name,
    encounter_provider.title as encounter_provider_title,
    call_communication_tracking.call_commn_dt as call_date,
    call_employee.ad_login as contact_employee_ad_login,
    call_employee.full_nm as contact_employee_name,
    regexp_extract(call_employee.title, '[^;]+(?=;)') as contact_employee_description,
    case
        when commn_id is not null then call_provider.title
        else encounter_provider.title
    end as contact_employee_title,
    case
        when lower(contact_employee_title) in ('crnp', 'pa', 'pa-c') then 1
        when
            lower(
                (case when commn_id  is not null then call_provider.user_id else encounter_provider.user_id end)
            ) in ('beeglen', 'schenknj') then 1
        else 0
    end as app_ind,
    dim_call_type.dim_call_type_id as call_type_id,
    dim_call_type.call_type_name,
    dim_call_outcome.call_outcome_id,
    lower(dim_call_outcome.call_outcome_nm) as call_outcome_name,
    dim_call_commn_type.call_commn_type_id,
    dim_call_commn_type.call_commn_type_nm as call_commn_type_name,
    count(distinct call_communication_tracking.commn_id) over (
        partition by stg_encounter.visit_key
    ) as total_calls,
    row_number() over (
        partition by stg_encounter.visit_key
        order by call_communication_tracking.commn_id
    ) as seq_num,
    row_number() over (
        partition by stg_encounter.visit_key, dim_call_type.dim_call_type_id
        order by call_communication_tracking.commn_id
    ) as in_out_seq_num,
    extract(
        epoch from call_communication_tracking.call_commn_dt - stg_encounter.encounter_date
    ) / 60 / 60 / 24 as days_from_first_contact,
--within thread
    lead(call_communication_tracking.call_commn_dt, 1) over (
        partition by stg_encounter.visit_key
        order by call_communication_tracking.call_commn_dt, call_communication_tracking.commn_id
    ) as next_call_date,
    extract(
        epoch from next_call_date - call_communication_tracking.call_commn_dt
    ) / 60.0 / 60 / 24 as n_days_to_next_call,
    stg_encounter.pat_key,
    stg_encounter.prov_key as encounter_prov_key,
    call_employee.emp_key as contact_employee_emp_key,
    call_provider.prov_key as contact_employee_prov_key
from
    {{ref('stg_encounter')}} as stg_encounter
inner join
    {{ref('stg_patient')}} as stg_patient on
        stg_patient.pat_key = stg_encounter.pat_key
inner join
    {{source('cdw', 'provider')}} as encounter_provider on
        stg_encounter.prov_key = encounter_provider.prov_key
left join
    {{source('cdw', 'employee')}} as encounter_employee on
        stg_encounter.prov_key = encounter_employee.prov_key
        and encounter_employee.ad_login is not null
left join
    {{source('cdw', 'call_reference_patient')}} as call_reference_patient on
        stg_encounter.visit_key = call_reference_patient.visit_key
left join
    {{source('cdw', 'call_communication_tracking')}} as call_communication_tracking on
        call_reference_patient.commn_trckng_key = call_communication_tracking.commn_trckng_key
left join
    {{source('cdw', 'dim_call_commn_type')}} as dim_call_commn_type on
        call_communication_tracking.dim_call_commn_type_key = dim_call_commn_type.dim_call_commn_type_key
        and dim_call_commn_type.call_commn_type_id != 2 -- not fax
left join
    {{source('cdw', 'dim_call_outcome')}} as dim_call_outcome on
        call_communication_tracking.dim_call_outcome_key = dim_call_outcome.dim_call_outcome_key
        and dim_call_outcome.call_outcome_id != 4 -- canceled
left join
    {{source('cdw', 'dim_call_type')}} as dim_call_type on
        call_communication_tracking.dim_call_type_key = dim_call_type.dim_call_type_key
        and dim_call_type.dim_call_type_id in (1, 2) -- incoming, outgoing
--who called
left join
    {{source('cdw', 'employee')}} as call_employee on
        call_communication_tracking.commn_entered_emp_key = call_employee.emp_key
        and call_employee.ad_login is not null
left join
    {{source('cdw', 'provider')}} as call_provider on
        call_employee.prov_key = call_provider.prov_key
where
    stg_encounter.department_name like '%ORTHOP%'
    --telephone, email, mychart
    and stg_encounter.encounter_type_id in (70, 305, 61)
    and stg_encounter.encounter_date < current_timestamp
