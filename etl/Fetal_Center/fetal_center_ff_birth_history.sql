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
	ff_account.id as account_id,
	ff_account.name as baby_full_name,
	ff_account.first_name__c as baby_first_name,
	ff_account.last_name__c as baby_last_name,
	ff_account.birthdate__c as baby_dob,
	ff_account.mrn__c as baby_mrn,
	to_timestamp(
		strleft(regexp_replace(ff_account.createddate, 'T', ' '), 19),
		'YYYY-MM-DD HH24:MI:SS'
	) as created_date,
	ff_account.diagnosis__c as diagnosis_id,
	intake.diagnosis_code__c as diagnosis_code,
	intake.current_diagnosis_category__c as current_diagnosis_category,
	intake.name as pregnancy_id,
	intake.account__c as mother_account_id,
	intake.estimated_date_of_delivery__c as estimated_date_of_delivery,
	intake.intake_name__c as intake_name,
	intake.pregnancy_outcome__c as pregnancy_outcome,
	intake.delivery_date__c as delivery_date,
	mother_account.name as mother_name,
	mother_account.first_name__c as mother_first_name,
	mother_account.last_name__c as mother_last_name,
	mother_account.birthdate__c as mother_dob,
	mother_account.mrn__c as mother_mrn
from sf_account as ff_account
left join sf_chop_intake__c as intake
	on ff_account.intake__c = intake.id
left join sf_account as mother_account
	on intake.account__c = mother_account.id
where ff_account.intake__c is not null and ff_account.patient_type__c = 'Baby'
