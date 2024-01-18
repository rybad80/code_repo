{{ config(
    materialized='table',
    dist='visit_key',
    meta = {
        'critical': true
    }
) }}

select
    visit.visit_key,
    visit.pat_key,
    visit.pat_id,
    cast(
        coalesce(visit.eff_dt, visit.hosp_admit_dt, visit.appt_dt, dim_date.full_date) as date
    ) as encounter_date,
    visit.hosp_admit_dt as hospital_admit_date,
    visit.hosp_dischrg_dt as hospital_discharge_date,
    pat_enc.enc_instant,
	pat_enc.enc_close_time,
	pat_enc.enc_closed_user_id
from
    {{source('cdw', 'visit')}} as visit
left join {{ref('dim_date')}} as dim_date
    on dim_date.date_key = visit.contact_dt_key
left join {{ source('clarity_ods', 'pat_enc') }} as pat_enc
	on pat_enc.pat_enc_csn_id = visit.enc_id
