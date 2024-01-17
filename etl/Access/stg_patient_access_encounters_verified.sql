{{ config(materialized='table', dist='pat_enc_csn_id') }}

--This query pulls in an indicator for encounters that had at least
--one verification in the three days leading up to an appointment.
with verified_enc as (
select
    reg_hx_open_pat_csn,
    1 as verified_ind
from {{source('clarity_ods', 'reg_hx')}} as reg_hx
    inner join {{ref('stg_encounter')}} as visit
        on visit.csn = reg_hx.reg_hx_open_pat_csn
where
    reg_hx_event_c in (31, 35)
    and reg_hx_inst_utc_dttm >= appointment_date - 3
    and appointment_date < current_date
group by
    reg_hx_open_pat_csn,
    verified_ind
)

select
    visit.enc_id as pat_enc_csn_id,
    verified_ind,
    1 as encounter_to_be_verified_ind
from {{source('cdw', 'visit')}} as visit
    inner join {{source('cdw', 'cdw_dictionary')}} as enc_type
        on enc_type.dict_key = visit.dict_enc_type_key
    inner join {{source('cdw', 'cdw_dictionary')}} as appt_stat
        on appt_stat.dict_key = visit.dict_appt_stat_key
    left join verified_enc
        on verified_enc.reg_hx_open_pat_csn = visit.enc_id
where
    appt_dt < current_date
    and appt_stat.src_id not in (3, 4)
    and enc_type.src_id = 101
