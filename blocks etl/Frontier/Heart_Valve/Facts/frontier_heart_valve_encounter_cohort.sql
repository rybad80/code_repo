with
heart_valve_build as (
    select
        visit_key,
        mrn,
        patient_name,
        1 as heart_valve_cath_ind,
        0 as heart_valve_trans_cath_ind,
        0 as heart_valve_cta_ind,
        0 as heart_valve_echo_ind,
        0 as heart_valve_mri_ind,
        0 as heart_valve_notes_ind,
        0 as heart_valve_surgery_ind
    from
        {{ ref('stg_heart_valve_cath') }}
    union all
    select
        visit_key,
        mrn,
        patient_name,
        0 as heart_valve_cath_ind,
        1 as heart_valve_trans_cath_ind,
        0 as heart_valve_cta_ind,
        0 as heart_valve_echo_ind,
        0 as heart_valve_mri_ind,
        0 as heart_valve_notes_ind,
        0 as heart_valve_surgery_ind
    from
        {{ ref('stg_heart_valve_trans_cath') }}
    union all
    select
        visit_key,
        mrn,
        patient_name,
        0 as heart_valve_cath_ind,
        0 as heart_valve_trans_cath_ind,
        1 as heart_valve_cta_ind,
        0 as heart_valve_echo_ind,
        0 as heart_valve_mri_ind,
        0 as heart_valve_notes_ind,
        0 as heart_valve_surgery_ind
    from
        {{ ref('stg_heart_valve_cta') }}
    union all
    select
        visit_key,
        mrn,
        patient_name,
        0 as heart_valve_cath_ind,
        0 as heart_valve_trans_cath_ind,
        0 as heart_valve_cta_ind,
        1 as heart_valve_echo_ind,
        0 as heart_valve_mri_ind,
        0 as heart_valve_notes_ind,
        0 as heart_valve_surgery_ind
    from
        {{ ref('stg_heart_valve_echo') }}
    union all
    select
        visit_key,
        mrn,
        patient_name,
        0 as heart_valve_cath_ind,
        0 as heart_valve_trans_cath_ind,
        0 as heart_valve_cta_ind,
        0 as heart_valve_echo_ind,
        1 as heart_valve_mri_ind,
        0 as heart_valve_notes_ind,
        0 as heart_valve_surgery_ind
    from
        {{ ref('stg_heart_valve_mri') }}
    union all
    select
        stg_heart_valve_notes.visit_key,
        stg_heart_valve_notes.mrn,
		coalesce(stg_patient_ods.patient_name,
            cardiac_valve_center.patient_name) as patient_name,
        0 as heart_valve_cath_ind,
        0 as heart_valve_trans_cath_ind,
        0 as heart_valve_cta_ind,
        0 as heart_valve_echo_ind,
        0 as heart_valve_mri_ind,
        1 as heart_valve_notes_ind,
        0 as heart_valve_surgery_ind
    from
        {{ ref('stg_heart_valve_notes') }} as stg_heart_valve_notes
		left join {{ ref('stg_patient_ods') }} as stg_patient_ods
			on stg_heart_valve_notes.mrn = stg_patient_ods.mrn
        left join {{ ref('cardiac_valve_center') }} as cardiac_valve_center
            on stg_heart_valve_notes.visit_key = cardiac_valve_center.record_id
    union all
    select
        visit_key,
        mrn,
        patient_name,
        0 as heart_valve_cath_ind,
        0 as heart_valve_trans_cath_ind,
        0 as heart_valve_cta_ind,
        0 as heart_valve_echo_ind,
        0 as heart_valve_mri_ind,
        0 as heart_valve_notes_ind,
        1 as heart_valve_surgery_ind
    from
        {{ ref('stg_heart_valve_surgery') }}
),
heart_valve_key as (
    select
        visit_key,
        mrn,
        patient_name,
        max(case when heart_valve_cath_ind = 1 then 1 else 0 end)
        as heart_valve_cath_ind,
        max(case when heart_valve_trans_cath_ind = 1 then 1 else 0 end)
        as heart_valve_trans_cath_ind,
        max(case when heart_valve_cta_ind = 1 then 1 else 0 end)
        as heart_valve_cta_ind,
        max(case when heart_valve_echo_ind = 1 then 1 else 0 end)
        as heart_valve_echo_ind,
        max(case when heart_valve_mri_ind = 1 then 1 else 0 end)
        as heart_valve_mri_ind,
        max(case when heart_valve_notes_ind = 1 then 1 else 0 end)
        as heart_valve_notes_ind,
        max(case when heart_valve_surgery_ind = 1 then 1 else 0 end)
        as heart_valve_surgery_ind
    from
        heart_valve_build
    group by
        visit_key,
        mrn,
        patient_name
)
select
    heart_valve_key.visit_key,
    heart_valve_key.mrn,
    coalesce(stg_encounter.patient_name, cardiac_valve_center.patient_name) as patient_name,
    coalesce(stg_encounter.encounter_date, cardiac_valve_center.date_of_referral) as encounter_date,
    case
        when stg_encounter.encounter_date is null
            then year(add_months(cardiac_valve_center.date_of_referral, 6))
        else year(add_months(stg_encounter.encounter_date, 6)) end
    as fiscal_year,
    case
        when heart_valve_cath_ind = '1' then 'Received Cardiac Cath'
        when heart_valve_trans_cath_ind = '1' then 'Received Transcatheter Valve Placement'
        when heart_valve_cta_ind = '1'
            and heart_valve_surgery_ind = '1' then 'Received Surgery and CTA'
        when heart_valve_cta_ind = '1' then 'Received CTA'
        when heart_valve_echo_ind = '1' then 'Received Advanced Imaging Echo'
        when heart_valve_mri_ind = '1' then 'Received Cardiac MRI'
        when heart_valve_notes_ind = '1' then 'Received Consult'
        when heart_valve_surgery_ind = '1' then 'Received Surgery'
        end
    as patient_description,
    coalesce(initcap(stg_encounter.provider_name), referring_provider) as provider_name,
    stg_encounter.provider_id as provider_id,
    stg_encounter.department_name,
    stg_encounter.department_id,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    stg_encounter.csn,
    stg_encounter_inpatient.admission_department_group as admission_department,
    case when
        heart_valve_cath_ind
        + heart_valve_trans_cath_ind
        + heart_valve_cta_ind
        + heart_valve_echo_ind
        + heart_valve_mri_ind
        + heart_valve_notes_ind
        + heart_valve_surgery_ind > 1 then 'check' end
    as patient_check,
    heart_valve_key.heart_valve_cath_ind,
    heart_valve_key.heart_valve_trans_cath_ind,
    heart_valve_key.heart_valve_cta_ind,
    heart_valve_key.heart_valve_echo_ind,
    heart_valve_key.heart_valve_mri_ind,
    heart_valve_key.heart_valve_notes_ind,
    heart_valve_key.heart_valve_surgery_ind,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
from
    heart_valve_key
    left join {{ ref('stg_encounter') }} as stg_encounter
        on heart_valve_key.visit_key = stg_encounter.visit_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
        on stg_encounter.visit_key = stg_encounter_inpatient.visit_key
    left join {{ ref('cardiac_valve_center') }} as cardiac_valve_center
        on heart_valve_key.visit_key = cardiac_valve_center.record_id
            and cardiac_valve_center.mrn is null
where
    year(add_months(stg_encounter.encounter_date, 6)) > 2020
    or cardiac_valve_center.mrn is null
group by
    heart_valve_key.visit_key,
    heart_valve_key.mrn,
    stg_encounter.csn,
    stg_encounter.patient_name,
    cardiac_valve_center.patient_name,
    stg_encounter.encounter_date,
    cardiac_valve_center.date_of_referral,
    stg_encounter.provider_name,
    referring_provider,
    stg_encounter.provider_id,
    stg_encounter.department_name,
    stg_encounter.department_id,
    stg_encounter.visit_type,
    stg_encounter.visit_type_id,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    heart_valve_key.heart_valve_cath_ind,
    heart_valve_key.heart_valve_trans_cath_ind,
    heart_valve_key.heart_valve_cta_ind,
    heart_valve_key.heart_valve_echo_ind,
    heart_valve_key.heart_valve_mri_ind,
    heart_valve_key.heart_valve_notes_ind,
    heart_valve_key.heart_valve_surgery_ind,
    stg_encounter_inpatient.admission_department_group,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0)
