{{ config(meta = {
    'critical': true
}) }}

with cpt_code_history as (
    select
        proc_id,
        cpt_code,
        contact_date as start_date,
        lead(contact_date - interval '1 day', 1, null)
            over(partition by proc_id order by contact_date)
        as end_date
    from
        {{source('clarity_ods','clarity_eap_ot')}}
    where
        lower(cpt_code) not like '%.del' --deleted
)

select
    arpb_transactions.pat_enc_csn_id,
    coalesce(stg_encounter.visit_key, 0) as visit_key,
    arpb_transactions.patient_id,
    coalesce(stg_patient.pat_key, 0) as pat_key,
    arpb_transactions.proc_id,
    coalesce(procedure.proc_key, 0) as proc_key,
    coalesce(cpt_code_history.cpt_code, arpb_transactions.cpt_code) as cpt_cd,
    arpb_transactions.service_date as billing_service_date,
    dim_date.date_key as billing_service_date_key,
    arpb_transactions.serv_provider_id as billing_service_provider_id,
    coalesce(dim_provider.provider_key, '0') as billing_service_provider_key,
    stg_department_all.department_name as billing_department_name,
    stg_department_all.dept_key as billing_department_key,
    1 as pb_transaction_ind,
    current_timestamp as upd_dt
from
    {{source('clarity_ods', 'arpb_transactions')}} as arpb_transactions
    inner join {{ref('stg_department_all')}} as stg_department_all
        on stg_department_all.department_id = arpb_transactions.department_id
    left join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = arpb_transactions.pat_enc_csn_id
    left join {{ref('dim_date')}} as dim_date
        on dim_date.full_date = arpb_transactions.service_date
    left join {{ref('dim_provider')}} as dim_provider
        on dim_provider.prov_id = arpb_transactions.serv_provider_id
    left join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_id = arpb_transactions.patient_id
    left join {{source('cdw','procedure')}} as procedure
        on procedure.proc_id = arpb_transactions.proc_id
    left join cpt_code_history
        on cpt_code_history.proc_id = arpb_transactions.proc_id
        and date(arpb_transactions.service_date) between
            cpt_code_history.start_date and cpt_code_history.end_date
where
	arpb_transactions.tx_type_c = 1
	and arpb_transactions.void_date is null
group by
    arpb_transactions.pat_enc_csn_id,
     coalesce(stg_encounter.visit_key, 0),
    arpb_transactions.patient_id,
    coalesce(stg_patient.pat_key, 0),
    arpb_transactions.proc_id,
    coalesce(procedure.proc_key, 0),
    coalesce(cpt_code_history.cpt_code, arpb_transactions.cpt_code),
    arpb_transactions.service_date,
    dim_date.date_key,
    arpb_transactions.serv_provider_id,
    coalesce(dim_provider.provider_key, '0'),
    stg_department_all.department_name,
    stg_department_all.dept_key
