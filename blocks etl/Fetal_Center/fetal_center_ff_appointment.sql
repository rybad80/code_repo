with sf_account as (
    select *
	from (
        select *, row_number() over(partition by id order by id) as row_num
        from {{ source('salesforce_ods', 'salesforce_account') }} ) as ff_account
    where row_num = 1
),
sf_chop_intake as (
    select *
	from (
        select *, row_number() over(partition by id order by id) as row_num
        from {{ source('salesforce_ods', 'chop_intake__c') }} ) as intake
    where row_num = 1
),
sf_appointment as (
    select *
	from (
        select *, row_number() over(partition by id order by id) as row_num
        from {{ source('salesforce_ods', 'appointment__c') }} ) as appointment
    where row_num = 1
)
select
    appointment.id as appointment_id,
    appointment.name as appointment_name,
    to_timestamp(
        strleft(regexp_replace(appointment.createddate, 'T', ' '), 19),
        'YYYY-MM-DD HH24:MI:SS'
    ) as created_date,
    appointment.account__c as account_id,
    case when length(trim(appointment.appointmentdateonly__c)) >= 8
        then to_date(appointment.appointmentdateonly__c, 'MM/DD/YYYY')
        else null end as appointment_date,
    appointment.appointment_date_time__c as appointment_date_time,
    appointment.appointment_procedure__c as appointment_procedure,
    appointment.appointment_status__c as appointment_status,
    appointment.chop_codes__c as chop_codes_id,
    appointment.intake__c as intake_id,
    appointment.unavailable__c as unavailable,
    ff_account.name as full_name,
    ff_account.source__c as account_source,
    ff_account.first_name__c as first_name,
    ff_account.last_name__c as last_name,
    ff_account.mrn__c as mrn,
    ff_account.birthdate__c as dob,
    intake.diagnosis_code__c as diagnosis_code,
	intake.current_diagnosis__c as current_diagnosis,
    intake.current_diagnosis_category__c as current_diagnosis_category,
    intake.estimated_date_of_delivery__c as estimated_date_of_delivery,
    intake.intake_name__c as intake_name,
    intake.name as pregnancy_id,
    intake.pregnancy_outcome__c as pregnancy_outcome,
    intake.delivery_date__c as delivery_date
from sf_appointment as appointment
left join sf_account as ff_account
        on appointment.account__c = ff_account.id
left join sf_chop_intake as intake
        on appointment.intake__c = intake.id
where appointment.isdeleted = false
