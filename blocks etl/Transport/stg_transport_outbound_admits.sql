with
    outbound_admits as (
        select
            stg_transport_new_calls.comm_id,
            visit.visit_key,
            visit.hosp_admit_dt,
            visit.hosp_dischrg_dt,
            coalesce(hospital_account.disch_dt, now()) as end_dt,
            rank() over(partition by stg_transport_new_calls.comm_id order by visit.hosp_admit_dt desc) as rnk
        from
            {{ ref('stg_transport_new_calls') }} as stg_transport_new_calls
            inner join {{ source('cdw', 'patient') }} as patient
                on stg_transport_new_calls.mrn = patient.pat_mrn_id
            inner join {{ source('cdw', 'hospital_account') }} as hospital_account
                on patient.pat_key = hospital_account.pat_key
            inner join {{ source('cdw', 'cdw_dictionary') }} as cdw_dictionary
                on hospital_account.dict_acct_class_key = cdw_dictionary.dict_key
            inner join {{ source('cdw', 'visit') }} as visit
                on hospital_account.pri_visit_key = visit.visit_key
        where
            lower(stg_transport_new_calls.transport_type_raw) in ('outbound', 'intercampus')
            and stg_transport_new_calls.intake_date
                    between hospital_account.adm_dt and end_dt
            and visit.visit_key > 0
)
select distinct
    outbound_admits.comm_id,
    outbound_admits.visit_key,
    outbound_admits.hosp_admit_dt,
    outbound_admits.hosp_dischrg_dt
from outbound_admits
where outbound_admits.rnk = 1
