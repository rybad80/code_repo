with
dx_rule as (

    select distinct
        dx_key
    from
        {{ source('cdw', 'diagnosis') }}
    where
       lower(icd10_cd) in (
       't78.2xxa',
        't78.00xa',
        't78.05xa',
        't78.07xa',
        't78.02xa',
        't78.01xa',
        't80.52xa',
        't78.03xa',
        't78.04xa',
        't78.08xa',
        't78.06xa',
        't78.09xa',
        't80.59xa',
        't88.6xxa',
        't78.40xa'
        )
),

epi as (

    select distinct
        stg_encounter_ed.visit_key
    from
        {{ ref('stg_encounter_ed') }} as stg_encounter_ed
        inner join {{ ref('medication_order_administration') }} as medication_order_administration
            on medication_order_administration.visit_key = stg_encounter_ed.visit_key
    where
        medication_order_administration.administration_seq_number > 0
        and medication_order_administration.administration_date <= stg_encounter_ed.ed_discharge_date
        and (
            upper(medication_order_administration.medication_name) like 'EPINEPHRINE%'
            or upper(medication_order_administration.generic_medication_name) like 'EPINEPHRINE%'
            )
)

select
    stg_encounter_ed.visit_key,
    stg_encounter_ed.pat_key,
    max(case when procedure_order_clinical.cpt_code = '500PATH02' then 1 else 0 end) as pathway_ind,
    max(case when epi.visit_key is not null then 1 else 0 end) as epi_admin_ind,
    max(
        case when lower(diagnosis_encounter_all.diagnosis_name) like '%anaphylaxis%' then 1 else 0 end
    ) as anaph_dx_ind,
    'ANAPHYLAXIS' as cohort,
    case when anaph_dx_ind = 1 then 'ANAPHYLAXIS' else 'ALLERGIC REACTION' end as subcohort
from
    {{ ref('stg_encounter_ed') }} as stg_encounter_ed
    inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        on diagnosis_encounter_all.visit_key = stg_encounter_ed.visit_key
    inner join dx_rule
            on dx_rule.dx_key = diagnosis_encounter_all.dx_key
    left join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on procedure_order_clinical.visit_key = stg_encounter_ed.visit_key
        and procedure_order_clinical.cpt_code = '500PATH02'
    left join epi on epi.visit_key = stg_encounter_ed.visit_key
where
    stg_encounter_ed.ed_patients_seen_ind = 1
    and year(stg_encounter_ed.encounter_date) >= year(current_date) - 5
    and (diagnosis_encounter_all.ed_primary_ind = 1
        or diagnosis_encounter_all.ed_other_ind = 1)
group by
    stg_encounter_ed.visit_key,
    stg_encounter_ed.pat_key
having
    anaph_dx_ind = 1
    or (anaph_dx_ind = 0
        and (pathway_ind = 1 or epi_admin_ind = 1)
        )
