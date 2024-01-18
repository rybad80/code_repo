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
    case when stg_encounter_inpatient.visit_key is not null or lower(stg_encounter.patient_class) = 'inpatient'
        then 1 else 0 end as engin_inpatient_ind,
    case when op_enc_engin.visit_key is not null then 1 else 0 end as engin_visit_ind,
    case when op_enc_generic.visit_key is not null then 1 else 0 end as generic_visit_ind,
    case when surgery_enc.visit_key is not null then 1 else 0 end as surgery_ind,
    case when (stg_encounter_inpatient.visit_key is not null or lower(stg_encounter.patient_class) = 'inpatient')
        and inpat_encounter.visit_key is not null then 1
        else 0 end as inpat_consult_ind,
    case when lookup_fp_visit.id is not null then 1 else 0 end as new_visit_ind,
    case when eeg_enc.visit_key is not null then 1 else 0 end as eeg_ind,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    date_trunc('month', stg_encounter.encounter_date) as visual_month,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from {{ ref('stg_encounter') }} as stg_encounter
inner join {{source('cdw','provider')}} as provider
    on provider.prov_key = stg_encounter.prov_key
left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
    on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
left join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
    on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
left join {{ ref('stg_frontier_engin_op_enc_engin') }} as op_enc_engin
    on stg_encounter.visit_key = op_enc_engin.visit_key
    and op_enc_engin.appointment_status_id != 4 -- exclude 'no show'
left join {{ ref('stg_frontier_engin_op_enc_generic') }} as op_enc_generic
    on stg_encounter.visit_key = op_enc_generic.visit_key
    and op_enc_generic.appointment_status_id != 4 -- exclude 'no show'
left join {{ ref('stg_frontier_engin_inpat_encounter') }} as inpat_encounter
    on stg_encounter.visit_key = inpat_encounter.visit_key
left join {{ ref('stg_frontier_engin_surgery_enc') }} as surgery_enc
    on stg_encounter.visit_key = surgery_enc.visit_key
left join {{ ref('lookup_frontier_program_visit')}} as lookup_fp_visit
    on stg_encounter.visit_type_id = cast(lookup_fp_visit.id as nvarchar(20))
    and lookup_fp_visit.program = 'engin'
    and lookup_fp_visit.category = 'new patient visit'
    and lookup_fp_visit.active_ind = 1
left join {{ ref('stg_frontier_engin_eeg_enc') }} as eeg_enc
    on stg_encounter.visit_key = eeg_enc.visit_key
where op_enc_engin.visit_key is not null
    or op_enc_generic.visit_key is not null
    or ((stg_encounter_inpatient.visit_key is not null or lower(stg_encounter.patient_class) = 'inpatient')
        and inpat_encounter.visit_key is not null)
    or surgery_enc.visit_key is not null
    or eeg_enc.visit_key is not null
