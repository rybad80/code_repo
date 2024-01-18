{{ config(meta = {
    'critical': true
}) }}

with stg_protocol_disposition as (
select
    encounter_key,
    row_number() over(partition by visit_key order by seq_num desc) as row_num,
    coalesce(
        (case when (last_value(nurse_triage_ptcl_id) over(
                partition by encounter_key
                order by seq_num
                range between unbounded preceding and unbounded following
            )) = 0
            then lag(nurse_triage_ptcl_nm) over(
                partition by encounter_key
                order by seq_num
                )
            else last_value(nurse_triage_ptcl_nm) over(
                partition by encounter_key
                order by seq_num
                range between unbounded preceding and unbounded following
                )
        end),
        'No triage protocol selected')
    as last_protocol,
    last_value(disposition_name) over(
        partition by encounter_key
        order by seq_num
        range between unbounded preceding and unbounded following
    ) as last_disposition_name,
    last_value(phone_disp_time) over(
        partition by encounter_key
        order by seq_num
        range between unbounded preceding and unbounded following
    ) as last_disposition_time,
    last_value(free_text) over(
        partition by encounter_key
        order by seq_num
        range between unbounded preceding and unbounded following
    ) as last_disposition_detail
from {{ ref('stg_nurse_triage_protocol_disp') }}
),
protocol_disposition as (
select
    encounter_key,
    last_protocol,
    last_disposition_name,
    last_disposition_time,
    last_disposition_detail
from stg_protocol_disposition
where row_num = 1
),
disposition_159 as (
select
    encounter_key,
    1 as refer_ed_uc_no_appt
from {{ ref('stg_nurse_triage_protocol_disp') }}
where
    -- disposition_name = 'Refer to ED/UC due to no office appointments'
    disposition_id = 159
group by encounter_key
),
viral_protocols as (
select
    encounter_key,
    1 as viral_protocol_encounter
from {{ ref('stg_nurse_triage_protocol_disp') }}
where nurse_triage_ptcl_id in (
    1416,   -- COLDS-P-AH
    1837,   -- COLDS-P-OH
    1420,   -- COUGH-P-AH
    1871,   -- COUGH-P-OH
    1643,   -- FEVER - 3 MONTHS OR OLDER-P-AH
    11214,  -- FEVER - 3 MONTHS OR OLDER-P-OH
    1614,   -- FEVER BEFORE 3 MONTHS OLD-P-AH
    11226,  -- FEVER BEFORE 3 MONTHS OLD-P-OH
    2065,   -- FEVER-P-OH
    1529,   -- SORE THROAT-P-AH
    2436    -- SORE THROAT-P-OH
    )
group by encounter_key
),
reason as (
-- arbitrarily choosing the first visit reason
-- they are unordered in the source table with no associated timestamp
select
    encounter_key,
    visit_reason as first_visit_reason,
    num_visit_reasons
from {{ ref('stg_nurse_triage_visit_reason') }}
where seq_num = 1
)
select
    stg_encounter_nurse_triage.visit_key,
    stg_encounter_nurse_triage.encounter_key,
    stg_encounter_nurse_triage.csn,
    stg_encounter_nurse_triage.patient_key,
    stg_encounter_nurse_triage.mrn,
    stg_encounter_nurse_triage.encounter_date,
    stg_encounter_nurse_triage.encounter_instant,
    stg_encounter_nurse_triage.enc_hhmm_period_start,
    stg_encounter_nurse_triage.patient_name,
    stg_encounter_nurse_triage.dob,
    stg_encounter_nurse_triage.encounter_age,
    stg_encounter_nurse_triage.encounter_age_category,
    stg_encounter_nurse_triage.sex,
    stg_encounter_nurse_triage.preferred_language,
    stg_encounter_nurse_triage.language_group,
    stg_encounter_nurse_triage.interpreter_needed_ind,
    stg_encounter_nurse_triage.race_ethnicity,
    stg_encounter_nurse_triage.deceased_ind,
    stg_encounter_nurse_triage.texting_opt_in_ind,
    stg_encounter_nurse_triage.pc_active_department_name,
    stg_encounter_nurse_triage.pc_active_provider_name,
    stg_encounter_nurse_triage.department_id,
    stg_encounter_nurse_triage.department_name,
    stg_encounter_nurse_triage.site_name,
    coalesce(
        stg_transferred_encounters.recipient_dept_id, stg_encounter_nurse_triage.department_id
    ) as effective_department_id,
    coalesce(
        stg_transferred_encounters.recipient_dept_name, stg_encounter_nurse_triage.department_name
    ) as effective_department_name,
    coalesce(
        stg_transferred_encounters.recipient_dept_display_name, stg_encounter_nurse_triage.department_display_name
    ) as effective_dept_display_name,
    coalesce(
        stg_transferred_encounters.recipient_dept_display_name, stg_encounter_nurse_triage.site_name
    ) as effective_site_name,
    stg_encounter_nurse_triage.ahp_contract_dept_id as contract_dept_id,
    stg_encounter_nurse_triage.ahp_contract_dept_name as contract_dept_name,
    stg_encounter_nurse_triage.ahp_contract_dept_display as contract_dept_display_name,
    stg_encounter_nurse_triage.primary_care_triage_ind,
    stg_encounter_nurse_triage.external_encounter_ind,
    stg_encounter_nurse_triage.provider_key,
    stg_encounter_nurse_triage.provider_id,
    stg_encounter_nurse_triage.provider_name,
    stg_encounter_nurse_triage.provider_type,
    stg_encounter_nurse_triage.encounter_closed_ind,
    stg_encounter_nurse_triage.encounter_close_date,
    round((extract(
       epoch from (encounter_close_date - stg_encounter_nurse_triage.encounter_instant)) / 3600.0), 3)
    as encounter_length_hrs,
    days_between(stg_encounter_nurse_triage.encounter_date, date(encounter_close_date)) as encounter_days_to_close,
    stg_encounter_nurse_triage.close_provider_key,
    stg_encounter_nurse_triage.close_provider_name,
    stg_encounter_nurse_triage.close_provider_type,
    reason.first_visit_reason,
    reason.num_visit_reasons,
    protocol_disposition.last_protocol,
    protocol_disposition.last_disposition_name,
    protocol_disposition.last_disposition_time,
    protocol_disposition.last_disposition_detail,
    -- this disposition is not used by AH triage as they do not schedule appointments
    case when effective_department_id != 37
        then coalesce(disposition_159.refer_ed_uc_no_appt, 0)
        else null
    end as refer_ed_uc_no_appt,
    -- exclude encounters for external departments from indicator
    -- this DOES still include encounters from Global Patient Services (very few)
    case when external_encounter_ind = 0
        then coalesce(viral_protocols.viral_protocol_encounter, 0)
        else null
    end as viral_protocol_encounter_ind,
    stg_nurse_triage_next_encounter.next_sick_encounter_date,
    stg_nurse_triage_next_encounter.next_sick_appointment_date,
    stg_nurse_triage_next_encounter.next_sick_department_id,
    stg_nurse_triage_next_encounter.next_sick_department_name,
    stg_nurse_triage_next_encounter.next_sick_provider_id,
    stg_nurse_triage_next_encounter.next_sick_provider_name,
    stg_nurse_triage_next_encounter.next_sick_lag_days,
    stg_encounter_nurse_triage.pat_key,
    stg_encounter_nurse_triage.pat_id
from {{ ref('stg_encounter_nurse_triage') }} as stg_encounter_nurse_triage
    left join {{ ref('stg_transferred_encounters') }} as stg_transferred_encounters
        on stg_transferred_encounters.encounter_key = stg_encounter_nurse_triage.encounter_key
    left join {{ ref('stg_nurse_triage_next_encounter') }} as stg_nurse_triage_next_encounter
        on stg_nurse_triage_next_encounter.encounter_key = stg_encounter_nurse_triage.encounter_key
    left join protocol_disposition
        on protocol_disposition.encounter_key = stg_encounter_nurse_triage.encounter_key
    left join reason
        on reason.encounter_key = stg_encounter_nurse_triage.encounter_key
    left join disposition_159
        on disposition_159.encounter_key = stg_encounter_nurse_triage.encounter_key
    left join viral_protocols
        on viral_protocols.encounter_key = stg_encounter_nurse_triage.encounter_key
