with sf_account as (
    select *
    from (
        select *, row_number() over(partition by id order by id) as row_num
        from {{ source('salesforce_ods', 'salesforce_account') }} ) as ff_account
    where row_num = 1
),
sf_chop_intake__c as (
    select *
    from (
        select *, row_number() over(partition by id order by id) as row_num
        from {{ source('salesforce_ods', 'chop_intake__c') }} ) as intake
    where row_num = 1
)
select
    intake.name as pregnancy_id,
    intake.id as intake_id,
    ff_account.last_name__c || ', ' || ff_account.first_name__c as full_name,
    ff_account.source__c as account_source,
    ff_account.first_name__c as first_name,
    ff_account.last_name__c as last_name,
    to_timestamp(
        strleft(regexp_replace(intake.createddate, 'T', ' '), 19),
        'YYYY-MM-DD HH24:MI:SS'
    ) as created_date,
    ff_account.birthdate__c as dob,
    intake.mrn__c as mrn,
    intake.account__c as account_id,
    intake.has_transfer_of_care__c as has_transfer_of_care,
    intake.closedate__c as close_date,
    intake.current_diagnosis__c as current_diagnosis,
    to_date(strleft(intake.date_closed__c, 10), 'YYYY-MM-DD') as date_closed,
    intake.delivery_date__c as delivery_date,
    intake.diagnosis_code__c as diagnosis_code,
    intake.current_diagnosis_category__c as current_diagnosis_category,
    intake.estimated_date_of_delivery__c as estimated_date_of_delivery,
    intake.evaluation_date__c as evaluation_date,
    intake.referral_evaluation__c as referral_evaluation,
    intake.gestational_age__c as gestational_age,
    intake.gestational_age_at_intake__c as gestational_age_at_intake,
    intake.intake_name__c as intake_name,
    intake.last_menstrual_period__c as last_menstrual_period,
    intake.leadsource__c as lead_source,
    intake.multiple_birth_type__c as multiple_birth_type,
    intake.multiple_births__c as multiple_births,
    intake.pregnancy_outcome__c as pregnancy_outcome,
    intake.referral_diagnosis_description__c as referral_diagnosis_description,
    intake.referral_diagnosis__c as referral_diagnosis,
    intake.referral_postal_code__c as referral_postal_code,
    intake.referral_state_province__c as referral_state_province,
    intake.referral_country_code__c as referral_country_code,
    intake.stage__c as intake_stage,
    intake.current_diagnosis_description__c as current_diagnosis_description
from sf_chop_intake__c as intake
left join sf_account as ff_account
    on intake.account__c = ff_account.id
where intake.isdeleted = false
