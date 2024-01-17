with
other_visits as ( --region
    select
        stg_encounter.visit_key,
        max(case when
            ctis_surgery_encounter.visit_key is not null
            ----add procedure count into surgery type table
              then 1 else 0 end) as surgery_encounter_ind
    from
        {{ ref('stg_encounter') }} as stg_encounter
        left join {{ ref('ctis_imaging') }} as ctis_imaging
            on ctis_imaging.visit_key = stg_encounter.visit_key
        left join {{ ref('ctis_anesthesia_casting') }} as ctis_anesthesia_casting
            on ctis_anesthesia_casting.visit_key = stg_encounter.visit_key
        left join {{ ref('ctis_surgery_encounter') }} as ctis_surgery_encounter
            on ctis_surgery_encounter.visit_key = stg_encounter.visit_key
        left join {{ ref('ctis_outpatient_visits') }} as ctis_outpatient_visits
            on ctis_outpatient_visits.visit_key = stg_encounter.visit_key
    where
        coalesce(
            ctis_imaging.visit_key,
            ctis_anesthesia_casting.visit_key,
            ctis_surgery_encounter.visit_key,
            ctis_outpatient_visits.visit_key) is not null
    group by
        stg_encounter.visit_key
    --end region
)
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
    coalesce(other_visits.surgery_encounter_ind, 0) as surgery_encounter_ind,
    case
        when stg_encounter_inpatient.visit_key is not null
        then 1
        else 0
    end as inpatient_ind,
    ctis_registry.first_noted_date,
    ctis_registry.ortho_credit_ind,
    case when other_visits.visit_key is not null then 1 else 0 end as ctis_event_ind,
    ctis_registry.thoracic_insufficiency_syndrome_ind,
    ctis_registry.congenital_scoliosis_ind,
    ctis_registry.neuromuscular_scoliosis_ind,
    ctis_registry.syndromic_scoliosis_ind,
    ctis_registry.infantile_scoliosis_ind,
    ctis_registry.juvenile_scoliosis_ind,
    ctis_registry.congenital_rib_or_spine_ind,
    ctis_registry.growing_rod_adjustment_ind,
    ctis_registry.ctis_category,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
    date_trunc('month', stg_encounter.encounter_date) as visual_month
from
    {{ ref('ctis_registry') }} as ctis_registry
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on stg_encounter.pat_key = ctis_registry.pat_key
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
        on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
    left join other_visits
        on other_visits.visit_key = stg_encounter.visit_key
where
    ctis_registry.ortho_credit_ind = 1 -- all visits count included in the specialty visits
       or other_visits.visit_key is not null
