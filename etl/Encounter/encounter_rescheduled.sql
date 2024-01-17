select
    {{
        dbt_utils.surrogate_key([
            'stg_encounter.csn',
             'v2.enc_id'
            ])
    }} as encounter_rescheduled_key,
    stg_encounter.csn,
    stg_encounter.department_id,
    stg_encounter.mrn,
    COALESCE(v2.appt_stat, stg_encounter.appointment_status) as appointment_status,
    stg_encounter.visit_type,
    COALESCE(v2.appt_dt, stg_encounter.appointment_date) as appointment_date,
    COALESCE(v2.appt_cancel_dt, visit.appt_cancel_dt) as cancel_date,
    COALESCE(v2.appt_made_dt, visit.appt_made_dt) as appointment_made_date,
    provider.prov_id,
    dim_visit_cncl_rsn.visit_cncl_rsn_nm as cancel_reason,
    v2.enc_id as rescheduled_enc_id,
    case when v2.enc_id is null then 0 else 1 end as rescheduled_ind,
    COALESCE(stg_encounter_outpatient_raw.primary_care_ind, 0) as primary_care_ind,
    financial_clearance_encounter.financial_clearance_ind
from
    {{ref('financial_clearance_encounter')}} as financial_clearance_encounter
inner join
    {{ref('stg_encounter')}} as stg_encounter
        on financial_clearance_encounter.csn = stg_encounter.csn
inner join
    {{source('cdw', 'visit')}} as visit
        on stg_encounter.visit_key = visit.visit_key
inner join
    {{source('cdw', 'dim_visit_cncl_rsn')}} as dim_visit_cncl_rsn
        on visit.dim_visit_cncl_rsn_key = dim_visit_cncl_rsn.dim_visit_cncl_rsn_key
left join
    {{source('cdw', 'provider')}} as provider
        on stg_encounter.prov_key = provider.prov_key
left join
    {{source('cdw', 'visit')}} as v2
        on TRUNC(stg_encounter.csn) = v2.appt_sn
left join
    {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
        on stg_encounter_outpatient_raw.visit_key = stg_encounter.visit_key
where
    (
        case when stg_encounter.appointment_status_id != 3
            then TRUNC(v2.enc_id) != v2.appt_sn and v2.appt_sn is not null
            else stg_encounter.appointment_status_id = 3 --cancelled
        end
    )
    and LOWER(cancel_reason) in (
      'chop cancel - is project testing',
      'chop cancel - user error',
      'chop cancel - provider unavailable',
      'chop cancel - scheduling error (non-online)',
      'patient cancel - transportation',
      'chop cancel - scheduling error',
      'patient cancel - no reason given',
      'chop cancel - insurance/documentation',
      'patient cancel - pricing/cost',
      'patient cancel - scheduling conflict',
      'patient cancel - medical reason',
      'chop cancel - earlier chop appointment',
      'environmental - facility/weather',
      'patient cancel - appointment outside chop',
      'patient cancel - self-cancel (mychop, patiently, televox)'
    )
