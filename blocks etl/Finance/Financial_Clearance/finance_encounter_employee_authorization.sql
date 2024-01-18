with authorized_denied as (
    select
        financial_clearance_encounter.csn,
        stg_finance_referral_status.referral_id,
        verification.last_change_user_id
    from
        {{ref('stg_finance_referral_status')}} as stg_finance_referral_status
    inner join
        {{source('clarity_ods', 'pat_enc')}} as pat_enc
            on pat_enc.referral_id = stg_finance_referral_status.referral_id
    left join
        {{ref('financial_clearance_encounter')}} as financial_clearance_encounter
            on pat_enc.pat_enc_csn_id = financial_clearance_encounter.csn
    inner join
        {{source('clarity_ods', 'zc_rfl_status')}} as zc_rfl_status
            on zc_rfl_status.rfl_status_c = stg_finance_referral_status.referral_status_c
    left join
        {{source('clarity_ods', 'verification')}} as verification
            on pat_enc.pat_enc_csn_id = verification.enc_csn
            and verification.verification_type_c = 8
			and verification.verif_status_c = 1
    where
        financial_clearance_encounter.appointment_date >= current_date - interval '365 days'
        and referral_req_yn = 'Y'
        and lower(zc_rfl_status.name) in ('authorized', 'denied')
        and financial_clearance_encounter.appointment_date
            between stg_finance_referral_status.status_start_datetime
            and stg_finance_referral_status.status_end_datetime
),

financial_clearance_encounters as (
    select
        financial_clearance_encounter.mrn,
        financial_clearance_encounter.csn,
        referral.referral_id,
        financial_clearance_encounter.encounter_date,
        financial_clearance_encounter.appointment_date,
        clarity_dep.specialty,
        case
            when encounter_inpatient.visit_key is not null
                then encounter_inpatient.discharge_department_center_abbr
            else financial_clearance_encounter.department_center_abbr
        end as center,
        zc_rfl_status.name as rfl_status,
        financial_clearance_encounter.financial_clearance_ind
    from
        {{ref('financial_clearance_encounter')}} as financial_clearance_encounter
    left join
        {{source('clarity_ods', 'pat_enc')}} as pat_enc
            on financial_clearance_encounter.csn = pat_enc.pat_enc_csn_id
    inner join
        {{source('clarity_ods', 'clarity_dep')}} as clarity_dep
            on clarity_dep.department_id = financial_clearance_encounter.department_id
    left join
        {{ref('encounter_inpatient')}} as encounter_inpatient
            on financial_clearance_encounter.csn = encounter_inpatient.csn
    left join
        {{source('clarity_ods', 'referral')}} as referral
            on referral.referral_id = pat_enc.referral_id
    left join
        {{source('clarity_ods', 'zc_rfl_status')}} as zc_rfl_status
            on zc_rfl_status.rfl_status_c = referral.rfl_status_c
    where
        financial_clearance_encounter.appointment_date
            >= add_months(date_trunc('month', current_date), -25)
        and referral_req_yn = 'Y'
)

select
    financial_clearance_encounters.mrn,
    financial_clearance_encounters.csn,
    financial_clearance_encounters.referral_id,
    worker.worker_id,
    financial_clearance_encounters.encounter_date as contact_date,
    financial_clearance_encounters.appointment_date,
    financial_clearance_encounters.specialty,
    financial_clearance_encounters.center,
    financial_clearance_encounters.rfl_status as referral_status
    --nvl(authorized_denied.authorized_or_denied, 0) as authorized_or_denied_ind,
    --nvl(authorized_denied.authorized_or_denied_72_hrs, 0) as authorized_or_denied_72_hrs_ind
from
    financial_clearance_encounters
left join
    authorized_denied
        on authorized_denied.csn = financial_clearance_encounters.csn
left join
    --{{source('workday_ods', 'worker')}} as worker
    {{ref('worker')}} as worker
        on lower(authorized_denied.last_change_user_id) = worker.ad_login
 where
     financial_clearance_encounters.financial_clearance_ind = 1
