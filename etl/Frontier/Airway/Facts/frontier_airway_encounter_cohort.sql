select
    stg_encounter.visit_key,
    stg_encounter.csn,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.encounter_date,
    initcap(provider.full_nm) as provider_name,
    provider.prov_id as provider_id,
    stg_encounter.department_name,
    stg_encounter.department_id,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    stg_encounter.appointment_status,
    stg_encounter.appointment_status_id,
    case when stg_encounter_inpatient.visit_key is not null then 1 else 0 end as inpatient_ind,
    case when enc_inpat.visit_key is not null then 1 else 0 end as airway_inpatient_ind, --Airway specific Inpat
    case when enc_ov.visit_key is not null then 1 else 0 end as ov_ind,
    case when enc_proc.visit_key is not null then 1 else 0 end as procedure_ind,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    date_trunc('month', stg_encounter.encounter_date) as visual_month,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from {{ ref('stg_encounter')}} as stg_encounter
inner join {{source('cdw','provider')}} as provider
    on provider.prov_key = stg_encounter.prov_key
inner join {{ ref('stg_frontier_airway_cohort_base')}} as cohort_base
    on cohort_base.pat_key = stg_encounter.pat_key
left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
    on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
left join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
    on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
left join {{ ref('stg_frontier_airway_enc_procedure')}} as enc_proc
    on stg_encounter.visit_key = enc_proc.visit_key
left join {{ ref('stg_frontier_airway_enc_ov')}} as enc_ov
    on stg_encounter.visit_key = enc_ov.visit_key
left join {{ ref('stg_frontier_airway_enc_inpatient')}} as enc_inpat
    on stg_encounter.visit_key = enc_inpat.visit_key
where
    (enc_proc.visit_key is not null or enc_ov.visit_key is not null)
    and stg_encounter.encounter_date between '2017-07-01' and current_date
