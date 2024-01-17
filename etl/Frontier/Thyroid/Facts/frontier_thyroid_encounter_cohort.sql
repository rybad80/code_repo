select
    stg_encounter.visit_key,
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.encounter_date,
    coalesce(enc_other_prov_d.provider_id, provider.prov_id) as provider_id,
    initcap(coalesce(enc_other_prov_d.provider_name, provider.full_nm)) as provider_name,
    stg_encounter.department_id,
    stg_encounter.department_name,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.appointment_status_id,
    stg_encounter.appointment_status,
    case when stg_encounter_inpatient.visit_key is not null
        or lower(stg_encounter.patient_class) = 'inpatient'
        then 1 else 0 end as thyroid_inpatient_ind,
    dx_hx.thyroid_cancer_dx_date,
    case when enc_ov_endo.visit_key is not null
        or stg_encounter.visit_type_id in ('3289', --follow up thyroid research
                                            '3290' --new thyroid research
                                            ) then 1 else 0 end as center_visit_ind,
    case when enc_dx.visit_key is not null then 1 else 0 end as dx_visit_ind,
    case when enc_cpt.visit_key is not null then 1 else 0 end as surgery_visit_ind,
    case when enc_other_prov_e.visit_key is not null then 1 else 0 end as elect_providers_ind,
    case when enc_other_prov_e.elect_thyroid_ind = 1 then 1 else 0 end as elect_thyroid_ind,
    case when enc_other_prov_d.visit_key is not null then 1 else 0 end as developmental_therapeutics_ind,
    case when enc_procedure_visit.visit_key is not null then 1 else 0 end as procedure_visit_ind,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    date_trunc('month', stg_encounter.encounter_date) as visual_month
from {{ ref('stg_encounter')}} as stg_encounter
inner join {{source('cdw','provider')}} as provider
    on provider.prov_key = stg_encounter.prov_key
left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
    on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
left join {{ ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
    on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
left join {{ ref('stg_frontier_thyroid_enc_ov_endo') }} as enc_ov_endo
    on stg_encounter.visit_key = enc_ov_endo.visit_key
left join {{ ref('stg_frontier_thyroid_enc_dx') }} as enc_dx
    on stg_encounter.visit_key = enc_dx.visit_key
left join {{ ref('stg_frontier_thyroid_enc_cpt') }} as enc_cpt
    on stg_encounter.visit_key = enc_cpt.visit_key
left join {{ ref('stg_frontier_thyroid_enc_other_prov_e') }} as enc_other_prov_e
    on stg_encounter.visit_key = enc_other_prov_e.visit_key
left join {{ ref('stg_frontier_thyroid_enc_other_prov_d') }} as enc_other_prov_d
    on stg_encounter.visit_key = enc_other_prov_d.visit_key
left join {{ ref('stg_frontier_thyroid_enc_prcd_all') }} as enc_procedure_visit
    on stg_encounter.visit_key = enc_procedure_visit.visit_key
left join {{ ref('stg_frontier_thyroid_dx_hx') }} as dx_hx
    on stg_encounter.pat_key = dx_hx.pat_key
where
    ((coalesce(enc_ov_endo.visit_key,
            enc_dx.visit_key,
            enc_cpt.visit_key,
            enc_other_prov_e.visit_key,
            enc_other_prov_d.visit_key,
            enc_procedure_visit.visit_key) is not null
    and (not(stg_encounter_inpatient.visit_key is not null or lower(stg_encounter.patient_class) = 'inpatient')
        or (enc_cpt.visit_key is not null or enc_procedure_visit.rai_visit_ind = 1)
        )
    )
    or stg_encounter.visit_type_id in ('3289', --follow up thyroid research
                                        '3290' --new thyroid research
                                        )
    )
    and stg_encounter.encounter_date <= current_date
    and stg_encounter.appointment_status_id in (2, 6, -2) --COMPLETED, ARRIVED, NOT APPLICABLE
