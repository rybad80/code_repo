select
    stg_encounter.visit_key,
    stg_encounter.mrn,
    stg_encounter.csn,
    stg_encounter.patient_name,
    stg_encounter.encounter_date,
    initcap(provider.full_nm) as provider_name,
    provider.prov_id as provider_id,
    stg_encounter.department_name,
    stg_encounter.department_id,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    case
        when stg_encounter_inpatient.visit_key is not null
        then 1 else 0
    end as inpatient_ind,
    stg_encounter_inpatient.admission_department_group as admission_department,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    date_trunc('month', stg_encounter.encounter_date) as visual_month
from
    {{ ref('cardiac_patient') }} as cardiac_patient
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on cardiac_patient.pat_key = stg_encounter.pat_key
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
        on stg_encounter.visit_key = stg_encounter_inpatient.visit_key
where
      lymphatics_ind = 1
