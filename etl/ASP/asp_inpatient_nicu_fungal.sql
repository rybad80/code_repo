-- purpose: get NICU visits that have a positive culture result for fungal candida
-- granularity: one row per visit

with nicu_fungal_raw as (
    select
        adt_department.visit_key,
        adt_department.mrn,
        adt_department.hospital_admit_date,
        stg_patient.birth_weight_kg * 1000 as birth_weight_g,
        case when birth_weight_g < 750  then 'Y' else 'N' end as low_birth_weight_750_ind,
        case when birth_weight_g < 1000 then 'Y' else 'N' end as low_birth_weight_1000_ind,
        case when birth_weight_g < 1500 then 'Y' else 'N' end as low_birth_weight_1500_ind,
        procedure_order_clinical.procedure_order_id,
        procedure_order_clinical.specimen_taken_date,
        adt_department.enter_date,
        adt_department.exit_date,
        -- indicates whether a blood, urine, csf, or other culture returned positive for candida fungus in the NICU
        max(
            case
                when
                    lookup_asp_inpatient_nicu_fungal_culture.culture_group is not null
                    and lower(procedure_order_result_clinical.result_value) like '%candida%'
                    and procedure_order_result_clinical.abnormal_result_ind = 1
                then 1 else 0
        end) as positive_nicu_fungal_culture_ind,
        -- indicates whether a blood culture was taken in the NICU
        max(
            case
                when
                    lookup_asp_inpatient_nicu_fungal_culture.culture_group = 'Blood'
                    and procedure_order_clinical.specimen_taken_date is not null
                    and procedure_order_result_clinical.result_value is not null
                then 1 else 0
        end) as nicu_blood_culture_ind,
        -- indicates whether a urine culture was taken in the NICU
        max(
            case
                when
                    lookup_asp_inpatient_nicu_fungal_culture.culture_group = 'Urine'
                    and procedure_order_clinical.specimen_taken_date is not null
                    and procedure_order_result_clinical.result_value is not null
                then 1 else 0
        end) as nicu_urine_culture_ind,
        -- indicates whether a cerebral spinal fluid culture was taken in the NICU
        max(
            case
                when
                    lookup_asp_inpatient_nicu_fungal_culture.culture_group = 'CSF'
                    and procedure_order_clinical.specimen_taken_date is not null
                    and procedure_order_result_clinical.result_value is not null
                then 1 else 0
        end) as nicu_csf_culture_ind,
        -- indicates whether an other culture was taken in the NICU (and was taken from a sterile site source)
        max(
            case
                when
                    lookup_asp_inpatient_nicu_fungal_culture.culture_group = 'Other'
                    and procedure_order_clinical.specimen_taken_date is not null
                    and procedure_order_result_clinical.result_value is not null
                    and procedure_order_result_clinical.order_specimen_source in (
                        'Arterial Catheter.',
                        'Bladder',
                        'Bladder, Urine',
                        'Blood',
                        'Blood, Venous',
                        'Blood.',
                        'Central Catheter, Blood',
                        'Cerebrospinal Fluid',
                        'Cord Blood.',
                        'PIC Cath.',
                        'Pericardial Fluid.',
                        'Pericardial fluid',
                        'Peripheral, Blood',
                        'Peritoneal Fluid.',
                        'Peritoneal fluid',
                        'Pleural Fluid',
                        'Pleural Fluid.',
                        'Shunt Fluid',
                        'Shunt Fluid.',
                        'Synovial Fluid',
                        'Synovial Fluid.',
                        'Thoracentesis Fluid.',
                        'Umbilical catheter arterial, Blood',
                        'Umbilical Venous Cath',
                        'Urine, Catheter',
                        'Urine, Clean catch',
                        'Urine, Cystoscopy',
                        'Urine.'
                    )
                then 1 else 0
        end) as nicu_other_culture_ind,
        -- indicates whether any blood, urine, or csf culture was taken in the NICU
        case
            when
                nicu_blood_culture_ind = 1
                or nicu_urine_culture_ind = 1
                or nicu_csf_culture_ind = 1
                or nicu_other_culture_ind = 1
            then 1 else 0
        end as any_culture_ind,
        -- indicates whether any blood, urine, csf, or other culture returned positive in the NICU
        case
            when
                any_culture_ind = 1
                and positive_nicu_fungal_culture_ind = 1
            then 1 else 0
        end as positive_fungal_any_culture_ind,
        -- indicates whether a blood culture returned positive in the NICU
        case
            when
                nicu_blood_culture_ind = 1
                and positive_nicu_fungal_culture_ind = 1
            then 1 else 0
        end as positive_fungal_blood_culture_ind,
        -- indicates whether a urine culture returned positive in the NICU
        case
            when
                nicu_urine_culture_ind = 1
                and positive_nicu_fungal_culture_ind = 1
            then 1 else 0
        end as positive_fungal_urine_culture_ind,
        -- indicates whether a cerebral spinal fluid culture returned positive in the NICU
        case
            when
                nicu_csf_culture_ind = 1
                and positive_nicu_fungal_culture_ind = 1
            then 1 else 0
        end as positive_fungal_csf_culture_ind,
        -- indicates whether an other culture returned positive in the NICU
        case
            when
                nicu_other_culture_ind = 1
                and positive_nicu_fungal_culture_ind = 1
            then 1 else 0
        end as positive_fungal_other_culture_ind
    from
        {{ref('adt_department')}} as adt_department
        inner join {{ref('stg_patient')}} as stg_patient
            on adt_department.pat_key = stg_patient.pat_key
        inner join {{ref('procedure_order_clinical')}} as procedure_order_clinical
            on adt_department.visit_key = procedure_order_clinical.visit_key
        inner join {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
            on procedure_order_clinical.proc_ord_key = procedure_order_result_clinical.proc_ord_key
        inner join {{ref('lookup_asp_inpatient_nicu_fungal_culture')}} as lookup_asp_inpatient_nicu_fungal_culture
            on procedure_order_result_clinical.result_component_name
                = lookup_asp_inpatient_nicu_fungal_culture.result_component_name

    where
        adt_department.department_group_name = 'NICU'
        and adt_department.hospital_admit_date >= '20130101'
        and (
            procedure_order_clinical.specimen_taken_date >= adt_department.enter_date
            and procedure_order_clinical.specimen_taken_date <= adt_department.exit_date
        )

    group by
        adt_department.visit_key,
        adt_department.mrn,
        adt_department.hospital_admit_date,
        stg_patient.birth_weight_kg,
        procedure_order_clinical.procedure_order_id,
        procedure_order_clinical.specimen_taken_date,
        adt_department.enter_date,
        adt_department.exit_date,
        procedure_order_clinical.department_id,
        procedure_order_result_clinical.result_value,
        procedure_order_result_clinical.abnormal_result_ind,
        procedure_order_result_clinical.result_component_name
),

nicu_fungal_cohort as (
    select
        visit_key,
        mrn,
        hospital_admit_date,
        birth_weight_g,
        low_birth_weight_750_ind,
        low_birth_weight_1000_ind,
        low_birth_weight_1500_ind,
        max(positive_fungal_any_culture_ind)   as positive_fungal_any_culture_ind,
        max(positive_fungal_blood_culture_ind) as positive_fungal_blood_culture_ind,
        max(positive_fungal_urine_culture_ind) as positive_fungal_urine_culture_ind,
        max(positive_fungal_csf_culture_ind)   as positive_fungal_csf_culture_ind,
        max(positive_fungal_other_culture_ind) as positive_fungal_other_culture_ind

    from
        nicu_fungal_raw

    group by
        visit_key,
        mrn,
        hospital_admit_date,
        birth_weight_g,
        low_birth_weight_750_ind,
        low_birth_weight_1000_ind,
        low_birth_weight_1500_ind
)

select
    visit_key,
    mrn,
    hospital_admit_date,
    birth_weight_g,
    low_birth_weight_750_ind,
    low_birth_weight_1000_ind,
    low_birth_weight_1500_ind,
    positive_fungal_any_culture_ind,
    case
        when positive_fungal_any_culture_ind = 0
        then 'N' else 'Y'
    end as positive_fungal_any_culture_char,
    case
        when positive_fungal_blood_culture_ind = 0
        then 'N' else 'Y'
    end as positive_fungal_blood_culture_char,
    case
        when positive_fungal_urine_culture_ind = 0
        then 'N' else 'Y'
    end as positive_fungal_urine_culture_char,
    case
        when positive_fungal_csf_culture_ind = 0
        then 'N' else 'Y'
    end as positive_fungal_csf_culture_char,
    case
        when positive_fungal_other_culture_ind = 0
        then 'N' else 'Y'
    end as positive_fungal_other_culture_char

from
    nicu_fungal_cohort

where
    positive_fungal_any_culture_ind = 1

group by
    visit_key,
    mrn,
    hospital_admit_date,
    birth_weight_g,
    low_birth_weight_750_ind,
    low_birth_weight_1000_ind,
    low_birth_weight_1500_ind,
    positive_fungal_any_culture_ind,
    positive_fungal_blood_culture_ind,
    positive_fungal_urine_culture_ind,
    positive_fungal_csf_culture_ind,
    positive_fungal_other_culture_ind
