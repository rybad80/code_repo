with malignancy_patient as (
    select
        patient_mrn,
        max(
        case when
            lower(disease_category) in (
            'solid tumor',
            'lymphoma',
            'leukemia'
            )
            or lower(disease_classification) = 'myelodysplastic / myeloproliferative diseases'
            or lower(disease_name) like 'runx1%'
            then 1 else 0 end
        ) as malignancy_history_ind,
        min(transplant_date) as first_transplant_date
    from  {{source('ods','transplants')}}
    where transplant_date is not null
    group by patient_mrn
),

crosswalk_donor_selections as (
    select
        st_entity_record_relations.parent_record_id,
        donor_selections.donor_display_name,
        donor_selections.relationship,
        donor_selections.donor_match_grade
    from
        {{source('ods','st_entity_record_relations')}} as st_entity_record_relations
        inner join {{source('ods','donor_selections')}} as  donor_selections
            on st_entity_record_relations.child_record_id = donor_selections.id
    where
        st_entity_record_relations.parent_record_type = 'StCore::EntityFactory::ActiveRecord::Transplant'
        and st_entity_record_relations.child_record_type = 'StCore::EntityFactory::ActiveRecord::DonorSelection'
        and lower(donor_selections.selection_status) = 'selected'
)

select
    transplants.patient_name,
    transplants.patient_mrn,
    transplants.patient_dob,
    transplants.disease_name,
    transplants.disease_classification,
    transplants.disease_category,
    transplants.hct_type,
    transplants.first_hct,
    transplants.prior_hct_number,
    transplants.last_hct_date,
    transplants.last_hct_external,
    transplants.last_hct_type,
    transplants.donor_relation,
    transplants.type_and_relation,
    transplants.product_type_abbr,
    transplants.transplant_type,
    transplants.product_type,
    transplants.donor_name,
    transplants.disease_name_relapse,
    transplants.transplant_date,
    transplants.transplant_info,
    transplants.transplant_upn,
    transplants.txn,
    transplants.enrolled_ancillary_study_names,
    transplants.presentation_date,
    transplants.referral_institution,
    transplants.referral_name,
    transplants.workflow_name,
    crosswalk_donor_selections.donor_display_name,
    crosswalk_donor_selections.relationship,
    crosswalk_donor_selections.donor_match_grade,
    stg_patient.death_date,
    malignancy_patient.first_transplant_date,
    case when first_transplant_date = transplant_date then 1 else 0 end as first_transplant_date_ind,
    /* Auto:
        Disease categories: Leukemia, solid tumor, Lymphoma
        Transplant relationship: Self
        Workflow name does not contain "car"
    */
    case when lower(transplants.donor_relation) = 'self'
        and lower(transplants.disease_category) in (
            'leukemia',
            'solid tumor',
            'lymphoma'
        )
        and lower(transplants.workflow_name) not like '%car%'
        and lower(product_type_abbr) not like 't cell%'
        then 1 else 0 end
    as autologous_stem_cell_transplant_ind,
    /* Allo:
        Disease categories: Leukemia, solid tumor, Lymphoma OR
            Disease Classifiation = myelodysplastic/myeloproliferative OR
            Disease name = RUNX1
        Transplant relationship: Related or Unrelated (not self)
        Excludes haplo donor_match_grade
    */
    case when lower(transplants.donor_relation) not in (
        'self'
        )
        and donor_match_grade not in (
        'Haploidentical',
        '3/6',
        '4/8',
        '5/10',
        '5/8',
        '6/10'
        )
        and (
            lower(transplants.disease_category) in (
            'leukemia',
            'solid tumor',
            'lymphoma'
            )
            or lower(transplants.disease_classification) in (
            'myelodysplastic / myeloproliferative diseases'
            )
            or lower(transplants.disease_name) like 'runx1%'
            )
        and lower(product_type_abbr) not like 't cell%'
        then 1 else 0 end
    as allogeneic_matched_donor_transplant_ind,
    /* Haplo:
        Based on donor_match_grade
    */
    case when donor_match_grade in (
        'Haploidentical',
        '3/6',
        '4/8',
        '5/10',
        '5/8',
        '6/10'
    ) and lower(product_type_abbr) not like 't cell%'
    then 1 else 0 end
    as haplo_transplant_ind,
    case when (extract(epoch from transplant_date)
        - extract(epoch from patient_dob)) / (60.0 * 60.0 * 24.00 * 365.25) < 21 then 1 else 0 end
    as lt_21_ind,
    malignancy_patient.malignancy_history_ind
from
    {{source('ods','transplants')}} as transplants
    inner join malignancy_patient
        on transplants.patient_mrn = malignancy_patient.patient_mrn
    left join crosswalk_donor_selections
        on transplants.id = crosswalk_donor_selections.parent_record_id
    inner join {{ ref('stg_patient')}} as stg_patient
        on transplants.patient_mrn = stg_patient.mrn
where transplant_date >= '2015-01-01'
    and transplant_date is not null
    and transplant_date <= current_date
