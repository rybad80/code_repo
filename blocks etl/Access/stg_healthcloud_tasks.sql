with salesforce_hc_task as (
    select hc_task.id,
    hc_task.activitydate,
    hc_task.status,
    hc_task.ownerid,
    hc_task.isdeleted,
    hc_task.accountid,
    hc_task.isclosed,
    hc_task.createddate,
    hc_task.lastmodifieddate,
    hc_task.systemmodstamp,
    hc_task.isarchived,
    hc_task.calldisposition,
    hc_task.date_time_closed_hcm__c,
    hc_task.date_time_opened_hcm__c,
    hc_task.departmentpicklist_hcm__c,
    hc_task.owner_profile__c,
    hc_task.division_hcm__c,
    hc_task.reason_for_call_hcm__c,
    hc_task.sales_cloud_taskid_hcm__c,
    hc_task.upd_dt,
    hc_task.recordtypeid,
    hc_task.call_line_hcm__c,
    hc_task.secure_chat_sent__c,
    COALESCE(hc_task.whoid, hc_task.contact2_hcm__c) as whoid
    from {{source('salesforce_hc_ods', 'salesforce_hc_task')}} as hc_task
),

salesforce_hc_user as (
    select * from  {{source('salesforce_hc_ods', 'salesforce_hc_user')}}
),

salesforce_hc_contact as (
    select * from  {{source('salesforce_hc_ods', 'salesforce_hc_contact')}}
),

salesforce_hc_account as (
    select * from  {{source('salesforce_hc_ods', 'salesforce_hc_account')}}
)

select
salesforce_hc_task.id,
salesforce_hc_task.activitydate,
salesforce_hc_task.status,
salesforce_hc_task.ownerid,
salesforce_hc_task.isdeleted,
salesforce_hc_task.accountid,
salesforce_hc_task.isclosed,
'1970-01-01'::date
 + (salesforce_hc_task.createddate::bigint / 1000 * interval '1 second') as createddate,
'1970-01-01'::date
 + (salesforce_hc_task.lastmodifieddate::bigint / 1000 * interval '1 second') as lastmodifieddate,
'1970-01-01'::date
 + (salesforce_hc_task.systemmodstamp::bigint / 1000 * interval '1 second') as systemmodstamp,
salesforce_hc_task.isarchived,
salesforce_hc_task.calldisposition,
'1970-01-01'::date + (
    salesforce_hc_task.date_time_closed_hcm__c
)::bigint / 1000 * interval '1 second' as date_time_closed_hcm__c,
'1970-01-01'::date + (
    salesforce_hc_task.date_time_opened_hcm__c
)::bigint / 1000 * interval '1 second' as date_time_opened_hcm__c,
DATE('1970-01-01'::date
 + salesforce_hc_task.date_time_opened_hcm__c::bigint / 1000 * interval '1 second') as date_opened,
salesforce_task.department_orig__c as original_department, --need this to correct some migration errors
salesforce_hc_task.departmentpicklist_hcm__c,
salesforce_hc_task.owner_profile__c,
salesforce_hc_task.division_hcm__c,
salesforce_hc_task.reason_for_call_hcm__c,
salesforce_hc_task.sales_cloud_taskid_hcm__c,
salesforce_hc_task.upd_dt,
salesforce_hc_task.recordtypeid,
salesforce_hc_task.call_line_hcm__c,
salesforce_hc_task.secure_chat_sent__c,
case when date_time_closed_hcm__c is null then null else
(ROUND((date_time_closed_hcm__c::bigint) - (date_time_opened_hcm__c::bigint), -1) / 60000)::bigint
end as time_to_close_in_minutes__c,
salesforce_hc_user.name,
salesforce_hc_user.division,
salesforce_hc_user.department,
salesforce_hc_user.title,
salesforce_hc_contact.id as contact_id,
salesforce_hc_contact.name as contact_name,
case when salesforce_hc_contact.mailingpostalcode is null
then salesforce_hc_account.billingstreet else salesforce_hc_contact.mailingstreet end as contact_mailing_street,
case when salesforce_hc_contact.mailingpostalcode is null
then salesforce_hc_account.billingcity else salesforce_hc_contact.mailingcity end as contact_mailing_city,
case when salesforce_hc_contact.mailingpostalcode is null
then salesforce_hc_account.billingstate else salesforce_hc_contact.mailingcity end as contact_mailing_state,
COALESCE(salesforce_hc_contact.mailingpostalcode, salesforce_hc_account.billingpostalcode)
as contact_mailing_postal_code,
case when salesforce_hc_contact.mailingpostalcode is null
then salesforce_hc_account.billingcountry else salesforce_hc_contact.mailingcountry end as contact_mailing_country,
case when salesforce_hc_contact.mailingpostalcode is null
then salesforce_hc_account.billinglatitude
else salesforce_hc_contact.mailinglatitude end as contact_mailing_latitude,
case when salesforce_hc_contact.mailingpostalcode is null
then salesforce_hc_account.billinglongitude
else salesforce_hc_contact.mailinglongitude end as contact_mailing_longitude,
salesforce_hc_contact.phone as contact_phone,
salesforce_hc_contact.fax as contact_fax,
salesforce_hc_contact.email as contact_email,
salesforce_hc_contact.title as contact_title,
salesforce_hc_contact.department as contact_department,
salesforce_hc_contact.chop_employee_hcm__c as contact_chop_employee,
salesforce_hc_contact.chop_location_hcm__c as contact_chop_location,
salesforce_hc_contact.epic_id__c as contact_epic_id,
salesforce_hc_contact.account_name_hcm__c as contact_account_name,
salesforce_hc_contact.onekeyid__c
from salesforce_hc_task
left join salesforce_hc_user on salesforce_hc_task.ownerid = salesforce_hc_user.id
left join {{source('salesforce_ods', 'salesforce_task')}} as salesforce_task
on salesforce_hc_task.sales_cloud_taskid_hcm__c = salesforce_task.id
left join salesforce_hc_contact on salesforce_hc_task.whoid = salesforce_hc_contact.id
left join salesforce_hc_account on salesforce_hc_contact.accountid = salesforce_hc_account.id
where salesforce_hc_task.owner_profile__c = 'Access Center_HCM'
