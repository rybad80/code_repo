with
dx_hx as (
    select
        diagnosis_encounter_all.pat_key,
        diagnosis_encounter_all.visit_key,
        max(case when (diagnosis_encounter_all.icd9_code = '530.13'
                            and lower(diagnosis_encounter_all.icd10_code) = 'k20.0')
                        or lower(diagnosis_encounter_all.diagnosis_name) = 'eosinophilic esophagitis'
                    then 1 else 0 end) as eoe_ind,
        max(case when date_part('years', age(diagnosis_encounter_all.encounter_date, dob)) > 5
                        and (
                                (diagnosis_encounter_all.icd9_code = '558.42'
                                    or lower(diagnosis_encounter_all.icd10_code) = 'k52.82')
                                or (lower(diagnosis_encounter_all.diagnosis_name) like '%eosinophilic%colitis%'
                                    or lower(diagnosis_encounter_all.diagnosis_name) like '%colitis%eosinophilic%')
                            )
                    then 1 else 0 end) as ec_ind,
        max(case when
                diagnosis_encounter_all.icd9_code in('535.70', '535.71', '558.41')
                or lower(diagnosis_encounter_all.icd10_code) = 'k52.81'
              then 1 else 0 end) as eg_ind,
        max(case when (
                (diagnosis_encounter_all.icd9_code = '558.3'
                    and lower(diagnosis_encounter_all.diagnosis_name)
                        like 'food protein induced enterocolitis syndrome%')
                or lower(diagnosis_encounter_all.icd10_code) in ('k52.21', 'k52.22')
                        )
                    then 1 else 0 end) as fpies_ind,
        max(case when
            lower(diagnosis_encounter_all.icd9_code) in (
                '995.6',   -- anaphylactic reaction due to food
                '995.60',  -- variety of: anaphylactic reaction due to food
                '995.61',  -- variety of: peanut-induced anaphylaxis
                '995.62',  -- variety of: shellfish-induced anaphylaxis
                '995.63',  -- variety of: fruits or vegetables-induced anaphylaxis
                '995.64',  -- variety of: tree nuts or seeds-induced anaphylaxis
                '995.65',  -- variety of: seafood, fish-induced anaphylaxis
                '995.66',  -- variety of: food additive-induced anaphylaxis
                '995.67',  -- variety of: milk products-induced anaphylaxis
                '995.68',  -- variety of: egg-induced anaphylaxis
                '995.69',  -- variety of: wheat, soy, other-induced anaphylaxis
                '995.7',
                -- variety of: pfas, multiple/not elsewhere classified food allergies
                '995.0',   -- variety of anaphylactic diagnoses
                'v15.01',  -- peanut
                'v15.02',  -- dairy
                'v15.03',  -- egg
                'v15.04',  -- seafood
                'v15.05'   -- food
                )
                or lower(diagnosis_encounter_all.icd10_code) in (
                    --all possible dxs attached to these codes are (currently) kept intentionally
                    'l23.6', 'l27.2', 'l50.0', 't78.00x', 't78.40x',
                    't78.40xa', 't78.40xd', 't78.01xa', 't78.02xa', 't78.07xa',
                    't78.08xa', 't78.05x', 't78.1x', 't78.1xxd', 't78.1xxa',
                    'z91.01', 'z91.010', 'z91.011', 'z91.012', 'z91.013',
                    'z91.014', 'z91.015', 'z91.016', 'z91.017', 'z91.018',
                    'z91.019'
                    )
                    then 1 else 0 end) as ige_ind,
        max(case when
                (patient_allergy.alrg_desc is not null
                    and lower(patient_allergy.alrg_desc) not like '%no known%' -- above drop non-allergies
                    and lower(patient_allergy.alrg_desc) not like '%no %'
                    --'no drug,food,latex,contact,rad contrast,blood' / 'no other allergens identified per script'
                    and lower(cdw_dictionary.dict_nm) = 'active' -- above drops inactive allergies
                )
              then 1 else 0 end) as non_ige_exclusion_ind
    from {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        inner join {{source('cdw', 'patient_allergy')}} as patient_allergy
             on diagnosis_encounter_all.pat_key = patient_allergy.pat_key
        inner join {{source('cdw', 'cdw_dictionary')}} as cdw_dictionary
            on patient_allergy.dict_stat_key = cdw_dictionary.dict_key
    group by
        diagnosis_encounter_all.pat_key,
        diagnosis_encounter_all.visit_key

),
visit_hx as (
    select
        stg_encounter.visit_key,
        case when stg_encounter.visit_type_id = '1508'  -- ee new
                then 1 else 0 end as ee_new_ind,
        case when stg_encounter.visit_type_id = '1509'  -- ee follow up
                then 1 else 0 end as ee_fol_ind,
        case when stg_encounter.visit_type_id = '1517'  --fpies new
                then 1 else 0 end as fpies_new_ind,
        case when stg_encounter.visit_type_id = '1518'  --fpies fol
                then 1 else 0 end as fpies_fol_ind,
        case when --patient_allergy-departments
            lower(lookup_fp_fa_departments.department_type) = 'allergy'
                then 1 else 0 end as allergy_department_ind,
        case when --gi-departments
            lower(lookup_fp_fa_departments.department_type) = 'gastroenterology'
                then 1 else 0 end as gi_department_ind,
            case when -- gi-visit-type
                stg_encounter.visit_type_id in ('19225.001',    --'gi - pvt gi visit',`
                                                '4040',         --'npv gi'
                                                '282'           -- fol
                                                )
                then 1 else 0 end as gi_visit_ind,
        case when -- general visit-type
                lookup_fp_fa_visit.category = 'general visit'
                then 1 else 0 end as food_allergy_general_visit_ind,
        case when
            stg_encounter.encounter_type_id in ('50',     --appointment
                                                '101',    --office visit
                                                '152',    --outpatient
                                                '76'      --telemedicine
                                                )
                then 1 else 0 end as food_allergy_encounter_ind,
        case when
            --lookup_fp_fa_visit.category = 'food challenge: original ind'
            stg_encounter.visit_type_id in ('1507', --food challenge
                                            '1530', --food challenge
                                            '1512', --fpies chall
                                            '3152', --fpies food challenge
                                            '3129', --oit initiation food challenge
                                            '2709', --oit milestone challenge
                                            '3128', --oit intake
                                            '2994', --oit visit
                                            '3597', --oit palforzia visit
                                            '3596'  --oit palf initiation food challenge
                                            )  --food challenges
                then 1 else 0 end as food_challenge_ind,
        case when --oit visit and/or intake indicator
            stg_encounter.visit_type_id in ('3128', --'oit intake' **originally used**
                                            '2994'  --'oit visit' **originally used**
                                            )
                then 1 else 0 end as oit_visit_original_ind,
        case when --FPIES and oit food challenge total indicator
            lookup_fp_fa_visit.category = 'fpies: food-challenge'
            or lookup_fp_fa_visit.category = 'oit: initiation'
            or lookup_fp_fa_visit.category = 'oit: milestone'
            --dropped lines136-38 with Meg during call on 09/13
            --or lookup_fp_fa_visit.category = 'oit: intake'
            --or lookup_fp_fa_visit.category = 'oit: visit'
            --or lookup_fp_fa_visit.category = 'oit: palforzia visit'
            or lookup_fp_fa_visit.category = 'oit: palf initiation food challenge'
            or stg_encounter.visit_type_id in ( '1507', --food challenge
                                                '1530' --food challenge
                                                )
                then 1 else 0 end as food_challenge_total_ind,
        case when --FPIES food challenge indicator
            lookup_fp_fa_visit.category = 'fpies: food-challenge'
                then 1 else 0 end as fpies_food_challenge_ind,
        case when --OIT food challenge 'initiation' indicator
            lookup_fp_fa_visit.category = 'oit: initiation'
                then 1 else 0 end as oit_initiation_ind,
        case when --OIT food challenge 'milestone' indicator
            lookup_fp_fa_visit.category = 'oit: milestone'
                then 1 else 0 end as oit_milestone_ind,
        case when --OIT food challenge 'intake' indicator
            lookup_fp_fa_visit.category = 'oit: intake'
                then 1 else 0 end as oit_intake_ind,
        case when --OIT food challenge 'visit' indicator
            lookup_fp_fa_visit.category = 'oit: visit'
                then 1 else 0 end as oit_visit_ind,
        case when --OIT food challenge 'palforzia visit' indicator
            lookup_fp_fa_visit.category = 'oit: palforzia visit'
                then 1 else 0 end as oit_palforzia_ind,
        case when --OIT food challenge 'palf initiation food challenge' indicator
            lookup_fp_fa_visit.category = 'oit: palf initiation food challenge'
                then 1 else 0 end as oit_palf_initiation_ind

    from {{ref('stg_encounter')}} as stg_encounter
    inner join {{ ref('lookup_frontier_program_departments')}} as lookup_fp_fa_departments
        on stg_encounter.department_id = cast(lookup_fp_fa_departments.department_id as nvarchar(20))
        and lookup_fp_fa_departments.program = 'food-allergy'
        and lookup_fp_fa_departments.active_ind = 1
    inner join {{ ref('lookup_frontier_program_visit')}} as lookup_fp_fa_visit
        on stg_encounter.visit_type_id = cast(lookup_fp_fa_visit.id as nvarchar(20))
        and lookup_fp_fa_visit.program = 'food-allergy'
        and lookup_fp_fa_visit.active_ind = 1
    group by
        stg_encounter.visit_key,
        lookup_fp_fa_departments.department_type,
        lookup_fp_fa_visit.category,
        stg_encounter.visit_type_id,
        stg_encounter.encounter_type_id

),
stg_fa_cohort as (
    select distinct
        dx_hx.visit_key,
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
            then 1
            else 0
        end as inpatient_ind,
        dx_hx.eoe_ind,
        dx_hx.ec_ind,
        dx_hx.eg_ind,
        dx_hx.fpies_ind,
        dx_hx.ige_ind,
        visit_hx.ee_new_ind,
        visit_hx.ee_fol_ind,
        visit_hx.food_allergy_general_visit_ind,
        visit_hx.food_allergy_encounter_ind,
        visit_hx.food_challenge_ind,
        visit_hx.oit_visit_original_ind,
        visit_hx.food_challenge_total_ind,
        visit_hx.fpies_food_challenge_ind,
        visit_hx.oit_initiation_ind,
        visit_hx.oit_milestone_ind,
        visit_hx.oit_intake_ind,
        visit_hx.oit_visit_ind,
        visit_hx.oit_palforzia_ind,
        visit_hx.oit_palf_initiation_ind,
        stg_encounter.pat_key,
        coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
        year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
        date_trunc('month', stg_encounter.encounter_date) as visual_month
    from
        dx_hx
        left join visit_hx on dx_hx.visit_key = visit_hx.visit_key
        left join {{ ref('stg_encounter') }} as stg_encounter
            on dx_hx.visit_key = stg_encounter.visit_key
        left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
            on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
        left join {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
            on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
        left join {{source('cdw','provider')}} as provider
            on provider.prov_key = stg_encounter.prov_key
    where
        allergy_department_ind + gi_department_ind > 0
        and (
            (eoe_ind + ec_ind + eg_ind + fpies_ind > 0
                and (food_allergy_general_visit_ind + food_challenge_ind + oit_visit_original_ind > 0
                    or food_allergy_encounter_ind = 1
                    )
                and non_ige_exclusion_ind = 1
            )
            or (ige_ind = 1
                and (food_allergy_general_visit_ind + food_challenge_ind + oit_visit_original_ind > 0
                    or food_allergy_encounter_ind = 1
                    )
                )
            )

),
fa_cohort as (
    select *,
        row_number()over(
            partition by mrn
            order by encounter_date)
            as pat_visit_seq_num,
        row_number()over (
            partition by mrn, visit_type,
                year(add_months(encounter_date, 6))
            order by encounter_date)
            as pat_per_fy_seq_num,
        row_number()over(
            partition by mrn,
                year(add_months(encounter_date, 6))
            order by encounter_date)
            as visit_per_fy_seq_num
    from stg_fa_cohort

)
select * from fa_cohort
