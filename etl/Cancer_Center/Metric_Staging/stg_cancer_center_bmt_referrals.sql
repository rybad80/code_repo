with cohort as (
select
     *,
    --first referral date per transplant
    row_number() over(partition by patient_mrn, patient_dob, referral_date order by created_at) as row_num
from
    {{ source('ods', 'transplants') }}
where
    referral_date is not null
and intake_coordinator is not null
),
crosswalk_insurance as (
    select
        parent_record_id,
        --most recent insurance info per transplant
        row_number() over(partition by parent_record_id order by created_at desc) as row_num
    from
        {{ source('ods', 'st_entity_record_relations') }} as st_entity_record_relations
        inner join {{ source('ods', 'initial_insurance_approvals') }} as initial_insurance_approvals
            on initial_insurance_approvals.id = st_entity_record_relations.child_record_id
    where
        st_entity_record_relations.child_record_type
        = 'StCore::EntityFactory::ActiveRecord::InitialInsuranceApproval'
        and  st_entity_record_relations.parent_record_type = 'StCore::EntityFactory::ActiveRecord::Transplant'
),
crosswalk_referral_entity as (
    select
        parent_record_id,
        --most recent entity info per transplant
        row_number() over(partition by parent_record_id order by created_at desc) as row_num
    from
        {{ source('ods', 'st_entity_record_relations') }} as st_entity_record_relations
    inner join {{ source('ods', 'referral_entities') }} as referral_entities
        on referral_entities.id = st_entity_record_relations.child_record_id
    where
        child_record_type = 'StCore::EntityFactory::ActiveRecord::ReferralEntity'
        and parent_record_type = 'StCore::EntityFactory::ActiveRecord::Transplant'
)
select
   patient_mrn as mrn,
    patient_dob as dob,
    patient_name,
    referral_date,
    'Bone Marrow Transplant' as drill_down,
    {{
    dbt_utils.surrogate_key([
        'patient_mrn',
        'referral_date',
        'drill_down'
        ])
    }} as primary_key
from
    cohort as transplants
    inner join crosswalk_insurance
            on transplants.id = crosswalk_insurance.parent_record_id
            and crosswalk_insurance.row_num = 1 --most recent insurance info per transplant
    inner join crosswalk_referral_entity
        on transplants.id = crosswalk_referral_entity.parent_record_id
        and crosswalk_referral_entity.row_num = 1 --most recent entity info per transplant
where
   transplants.row_num = 1 --first referral date per transplant
