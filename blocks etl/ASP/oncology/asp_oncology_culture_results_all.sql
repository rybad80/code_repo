with
--find whether patient in the PICU 24 hours prior - 72 hours after culture order
picu_adt as (
    select distinct
        stg_asp_oncology_positive_cultures.positive_culture_key,
        1 as picu_ind
    from
        {{ ref('stg_asp_oncology_positive_cultures') }}
        as stg_asp_oncology_positive_cultures --noqa: L031
    inner join {{ ref('adt_department_group') }}
        as adt_department_group --noqa: L031
        on adt_department_group.visit_key
            = stg_asp_oncology_positive_cultures.visit_key
    where
        lower(adt_department_group.department_group_name) = 'picu'
        /*want to select all picu visits that occurred in the range of
        24 hours before to 72 hours after the culture order*/
        --patient did not exit the bed prior to the start of our order interval 
        and stg_asp_oncology_positive_cultures.placed_date - interval '24 hours'
        <= adt_department_group.exit_date_or_current_date
        --patient did not enter the bed after our order interval
        and stg_asp_oncology_positive_cultures.placed_date + interval '72 hours'
        >= adt_department_group.enter_date
),

inpatient as (--only include cultures where
    --patient admitted within 48 hours if not already inpatient
    select
        stg_asp_oncology_positive_cultures.positive_culture_key,
        first_value(encounter_inpatient.inpatient_admit_date)
        over (
            partition by stg_asp_oncology_positive_cultures.positive_culture_key
            order by encounter_inpatient.inpatient_admit_date
        ) as inpatient_admit_date,
        first_value(encounter_inpatient.admission_department_center_abbr)
        over (
            partition by stg_asp_oncology_positive_cultures.positive_culture_key
            order by encounter_inpatient.inpatient_admit_date
        ) as admission_department_center_abbr,
        first_value(encounter_inpatient.admission_department)
        over (
            partition by stg_asp_oncology_positive_cultures.positive_culture_key
            order by encounter_inpatient.inpatient_admit_date
        ) as admission_department,
        first_value(encounter_inpatient.admission_service)
        over (
            partition by stg_asp_oncology_positive_cultures.positive_culture_key
            order by encounter_inpatient.inpatient_admit_date
        ) as admission_service,
        first_value(encounter_inpatient.discharge_department)
        over (
            partition by stg_asp_oncology_positive_cultures.positive_culture_key
            order by encounter_inpatient.inpatient_admit_date
        ) as discharge_department,
        first_value(encounter_inpatient.discharge_service)
        over (
            partition by stg_asp_oncology_positive_cultures.positive_culture_key
            order by encounter_inpatient.inpatient_admit_date
        ) as discharge_service
    from
        {{ ref('stg_asp_oncology_positive_cultures') }}
        as stg_asp_oncology_positive_cultures --noqa: L031
    inner join {{ ref('encounter_inpatient') }}
        as encounter_inpatient --noqa: L031
        on encounter_inpatient.pat_key
            = stg_asp_oncology_positive_cultures.pat_key
            and stg_asp_oncology_positive_cultures.result_date
            between encounter_inpatient.inpatient_admit_date - interval '2 days'
            and coalesce(encounter_inpatient.hospital_discharge_date, now())
),

tumor as (--determine oncology dx
    select
        stg_asp_oncology_positive_cultures.positive_culture_key,
        min(
            --if BMT transplant before culture, then BMT. 
            --else check if ever Leuk/Lymph.
            case
                when
                    lower(
                        cancer_center_bmt_transplants.transplant_type
                    ) = 'autologous'
                    then 'BMT - Autologous'
                when
                    lower(
                        cancer_center_bmt_transplants.transplant_type
                    ) = 'allogeneic'
                    then 'BMT - Allogeneic'
                when (
                    --leukemia, non-hodgkins lymphoma, or  
                    --pre-malignant hematologic disorders
                    registry_tumor_oncology.onco_general_dx_cd in (10, 21, 220)
                    and registry_tumor_oncology.icdo_histology_behavior_cd
                    not in (
                        '8000/1', -- neoplasm, uncertain whether 
                        -- benign or malignant
                        '8003/3', -- malignant tumor, giant cell type
                        '8821/1', -- aggressive fibromatosis
                        '9041/3', -- synovial sarcoma, spindle cell
                        '9650/3', -- classical hodgkin lymphoma
                        '9651/3', -- hodgkin lymphoma, lymphocyte-rich
                        '9663/3', -- nodular sclerosis classical 
                        -- hodgkin lymphoma
                        '9700/3', -- mycosis fungoides (c44._)
                        '9708/3', -- subcutaneous panniculitis-like 
                        -- t-cell lymphoma
                        '9709/3', -- cutaneous t-cell lymphoma, nos (c44._)
                        '9718/3', -- primary cutaneous anaplastic large 
                        -- cell lymphoma (c44._)
                        '9751/1', -- langerhans cell histiocytosis, nos 
                        '9760/3', -- immunoproliferative disease, nos 
                        '9898/1', -- transient abnormal myelopoiesis
                        '9950/3', -- polycythemia vera (c42.1)
                        '9962/3', -- essential thrombocythemia (c42.1)
                        '9970/1', -- lymphoproliferative disorder, nos (c42.1)
                        '9971/1', -- post transplant 
                        --lymphoproliferative disorder, nos (ptld)
                        '9971/3', -- polymorphic post transplant 
                        -- lymphoproliferative disorder (ptld)
                        '9984/3', -- refractory anemia with excess 
                        -- blasts in transformation (c42.1)
                        'SD56/0', -- aplastic anemia chop-2849
                        'SD57/0', -- beta thalassemia chop-2850
                        'SD74/0'  -- neoplasm ruled out
                    )
                ) then 'Hematologic malignancy/MDS'
            end
        ) as dx_category
    from
        {{ ref('stg_asp_oncology_positive_cultures') }}
        as stg_asp_oncology_positive_cultures --noqa: L031
    left join
        {{ ref('cancer_center_bmt_transplants') }}
        as cancer_center_bmt_transplants --noqa: L031
        on cancer_center_bmt_transplants.patient_mrn
            = stg_asp_oncology_positive_cultures.mrn
            and cancer_center_bmt_transplants.transplant_date
            <= date(stg_asp_oncology_positive_cultures.specimen_taken_date)
    left join
        {{ source('cdw', 'registry_tumor_oncology') }}
        as registry_tumor_oncology --noqa: L031
        on registry_tumor_oncology.pat_key
            = stg_asp_oncology_positive_cultures.pat_key
    group by
        stg_asp_oncology_positive_cultures.positive_culture_key
),

neutropenia as (
    select
        stg_asp_oncology_positive_cultures.positive_culture_key,
        max(
            case
                when
                    procedure_order_result_clinical.result_value_numeric
                    < 200 then 1
                when
                    procedure_order_result_clinical.result_value_numeric
                    >= 200 then 0
            end
        ) as neutropenia_ind
    from
        {{ ref('stg_asp_oncology_positive_cultures') }}
        as stg_asp_oncology_positive_cultures --noqa: L031
    inner join
        {{ ref('procedure_order_result_clinical') }}
        as procedure_order_result_clinical --noqa: L031
        on procedure_order_result_clinical.pat_key
            = stg_asp_oncology_positive_cultures.pat_key
    where
        procedure_order_result_clinical.result_component_id in (
            66,        -- absolute neutrophils
            1959,      -- absolute neutrophils
            1993,      -- absolute band neutrophils
            2795,      -- neutrophils (absolute)
            5887,      -- neutrophils absolute
            7621,      -- neutrophils (absolute)-lc
            8594,      -- neutrophils absolute
            20105,     -- absolute band neutrophils
            20112,     -- absolute neutrophils-q
            999043,    -- absolute neutrophil count (osh result)
            3401555,   -- neutrophils absolute-lgh
            123050021, -- absolute neutrophils automated count
            123050045  -- absolute neutrophils-wam
        )
        and procedure_order_result_clinical.specimen_taken_date
        >= (
            stg_asp_oncology_positive_cultures.specimen_taken_date
            - interval '48 hours'
        )
        and procedure_order_result_clinical.specimen_taken_date
        <= (
            stg_asp_oncology_positive_cultures.specimen_taken_date
            + interval '48 hours'
        )
    group by
        stg_asp_oncology_positive_cultures.positive_culture_key
)

select
    stg_asp_oncology_positive_cultures.positive_culture_key,
    stg_asp_oncology_positive_cultures.procedure_order_organism,
    stg_asp_oncology_positive_cultures.procedure_order_id,
    stg_asp_oncology_positive_cultures.proc_ord_key,
    stg_asp_oncology_positive_cultures.pat_key,
    stg_asp_oncology_positive_cultures.mrn,
    stg_asp_oncology_positive_cultures.seq_num,
    stg_patient.race_ethnicity,
    stg_patient.sex,
    case
        when stg_encounter_chop_market.chop_market = 'international'
        then 1 else 0
    end as international_ind,
    neutropenia.neutropenia_ind,
    inpatient.inpatient_admit_date,
    inpatient.admission_department_center_abbr,
    inpatient.admission_department,
    stg_asp_oncology_positive_cultures.service
    as inclusion_service,
    inpatient.admission_service,
    inpatient.discharge_department,
    inpatient.discharge_service,
    stg_asp_oncology_positive_cultures.placed_date,
    stg_asp_oncology_positive_cultures.specimen_taken_date,
    stg_asp_oncology_positive_cultures.result_date,
    stg_asp_oncology_positive_cultures.organism,
    stg_asp_oncology_positive_cultures.antibiotic,
    stg_asp_oncology_positive_cultures.susceptibility,
    stg_asp_oncology_positive_cultures.sensitivity_value,
    stg_asp_oncology_positive_cultures.visit_key,
    coalesce(tumor.dx_category, 'Other') as dx_category,
    coalesce(picu_adt.picu_ind, 0) as picu_around_order_ind,

    case
        when inpatient.inpatient_admit_date + interval '2 days'
            <= stg_asp_oncology_positive_cultures.placed_date
            then 1 else 0
    end as hospital_onset_ind,

    case
        when
            --always interpreted as susceptible
            lower(stg_asp_oncology_positive_cultures.susceptibility) in (
                'deduced, susceptible',
                'sensitive',
                'sensitive*',
                'susceptible-dose dependent'
            )
            --result = 'see interpretation' only ever 
            --used when cefepime is susceptible
            or (
                lower(
                    stg_asp_oncology_positive_cultures.susceptibility
                ) like '%interpretation%'
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) = 'cefepime'
            )
            --result = 'see interpretation' used for 
            --susceptible and non-susceptible resutls 
            --for ceftriaxone, penecillin, and cefotaxime. 
            --Must use MIC (sensitivity_value)
            --to determine whether sensitivity is met
            or (
                lower(
                    stg_asp_oncology_positive_cultures.susceptibility
                ) like '%interpretation%'
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) = 'ceftriaxone'
                --MIC reorded as string. Remove all non-numeric characters
                and cast(
                    regexp_replace(
                        stg_asp_oncology_positive_cultures.sensitivity_value,
                        '[-<>=a-zA-z/ ]',
                        ''
                    ) as numeric(12, 3)
                ) <= 1
            )
            or (
                lower(
                    stg_asp_oncology_positive_cultures.susceptibility
                ) like '%interpretation%'
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) like '%penicillin%'
                --MIC reorded as string. Remove all non-numeric characters
                and cast(
                    regexp_replace(
                        stg_asp_oncology_positive_cultures.sensitivity_value,
                        '[-<>=a-zA-z/ ]',
                        ''
                    ) as numeric(12, 3)
                ) <= 2
            )
            or (
                lower(
                    stg_asp_oncology_positive_cultures.susceptibility
                ) like '%interpretation%'
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) like '%cefotaxime%'
                --MIC reorded as string. Remove all non-numeric characters
                and cast(
                    regexp_replace(
                        stg_asp_oncology_positive_cultures.sensitivity_value,
                        '[-<>=a-zA-z/ ]',
                        ''
                    ) as numeric(12, 3)
                ) <= 1
            )
            then 1
        when
            --always interpreted as non-susceptible
            lower(stg_asp_oncology_positive_cultures.susceptibility) in (
                'deduced, resistant',
                'intermediate',
                'neg',
                'negative',
                'not susceptible',
                'resistant',
                '* see interpretation below'
            )
            then 0 --'not applicable' and 'pos' should be null. 
    -- Cannot determine susceptibility
    end as susceptible_ind,

    --clinically relevant indicator
    case
        when lookup_abx.clinically_relevant = 1
            then 1
        else 0
    end as clinically_relevant_ind,

    --cefepime
    case
        when
            (--directly susceptible to cefepime
                lookup_abx.cefepime_gentamicin_penem_relevant = 1
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) = 'cefepime'
                and susceptible_ind = 1
            )
            or (--susceptible to cefepime by inference
                lookup_abx.cefotaxime_relevant = 1
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) = 'cefotaxime'
                and susceptible_ind = 1
            )
            or (--susceptible to cefepime by inference
                lookup_abx.ceftriaxone_relevant = 1
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) = 'ceftriaxone'
                and susceptible_ind = 1
            )
            or (--always susceptible to cefepime by nature
                lower(stg_asp_oncology_positive_cultures.organism) in (
                    'staphylococcus aureus',
                    'streptococcus agalactiae',
                    'streptococcus pyogenes'
                )
            )
            then 1
        else 0
    end as cefepime_numerator_ind,

    --cefepime + gentamicin
    case
        when
            cefepime_numerator_ind = 1
            or (--directly susceptible to gentamicin
                lookup_abx.cefepime_gentamicin_penem_relevant = 1
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) = 'gentamicin'
                and susceptible_ind = 1
            )
            then 1
        else 0
    end as cefepime_gentamicin_numerator_ind,

    --cefepime + vancomycin
    --always include cefepime_vancomycin_numerator_part1_ind in final numerator
    case
        when
            cefepime_numerator_ind = 1
            or (--directly susceptible to vancomycin
                lookup_abx.vancomycin_relevant = 1
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) = 'vancomycin'
                and susceptible_ind = 1
            )
            then 1
        else 0
    end as cefepime_vancomycin_numerator_part1_ind,

    --Only include in final numerator if both 
    --cefepime_vancomycin_numerator_part2a_ind = 1
    --and cefepime_vancomycin_numerator_part2b_ind = 1.  
    --These organisms must be susceptible to vancomycin 
    --AND NOT susceptible to cefotaxime.
    case
        when
            lookup_abx.cefotaxime_relevant = 1
            and lower(
                stg_asp_oncology_positive_cultures.antibiotic
            ) = 'vancomycin'
            and susceptible_ind = 1
            then 1
        else 0
    end as cefepime_vancomycin_numerator_part2a_ind,

    --only include in final numerator if both 
    --cefepime_vancomycin_numerator_part2a_ind = 1
    --and cefepime_vancomycin_numerator_part2b_ind = 1    
    case
        when
            lookup_abx.cefotaxime_relevant = 1
            and lower(
                stg_asp_oncology_positive_cultures.antibiotic
            ) = 'cefotaxime'
            and susceptible_ind = 0
            then 1
        else 0
    end as cefepime_vancomycin_numerator_part2b_ind,

    --meropenem OR imipenem + vancomycin
    case
        when
            (--directly susceptible to imipenem, meropenem, ertapenem
                lookup_abx.cefepime_gentamicin_penem_relevant = 1
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) in ('imipenem', 'meropenem', 'ertapenem')
                and susceptible_ind = 1
            )
            or (--susceptible by inference
                lookup_abx.ceftriaxone_relevant = 1
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) = 'ceftriaxone'
                and susceptible_ind = 1
            )
            or (--always susceptible by nature
                lower(stg_asp_oncology_positive_cultures.organism) in (
                    'staphylococcus aureus',
                    'streptococcus agalactiae',
                    'streptococcus pyogenes'
                )
            )
            or (--directly susceptible to vancomycin
                max(
                    lookup_abx.vancomycin_relevant,
                    lookup_abx.cefotaxime_relevant
                ) = 1
                and lower(
                    stg_asp_oncology_positive_cultures.antibiotic
                ) = 'vancomycin'
                and susceptible_ind = 1
            )
            then 1
        else 0
    end as meropenem_imipenem_numerator_ind
from
    {{ ref('stg_asp_oncology_positive_cultures') }}
    as stg_asp_oncology_positive_cultures --noqa: L031
inner join {{ ref('stg_patient') }} as stg_patient --noqa: L031
    on stg_patient.pat_key = stg_asp_oncology_positive_cultures.pat_key
inner join inpatient
    on inpatient.positive_culture_key
        = stg_asp_oncology_positive_cultures.positive_culture_key
left join {{ ref('stg_encounter_chop_market') }} as stg_encounter_chop_market
    on stg_encounter_chop_market.visit_key = stg_asp_oncology_positive_cultures.visit_key
left join picu_adt
    on picu_adt.positive_culture_key
        = stg_asp_oncology_positive_cultures.positive_culture_key
left join {{ ref('lookup_asp_oncology_organism_abx_relevance') }}
    as lookup_abx --noqa: L031
    on lower(lookup_abx.organism)
        = lower(stg_asp_oncology_positive_cultures.organism)
inner join tumor on tumor.positive_culture_key
        = stg_asp_oncology_positive_cultures.positive_culture_key
inner join neutropenia
    on neutropenia.positive_culture_key
        = stg_asp_oncology_positive_cultures.positive_culture_key
