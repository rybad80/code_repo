select
salesforce_task.id,
salesforce_task.activitydate,
salesforce_task.status,
salesforce_task.ownerid,
salesforce_task.isdeleted,
salesforce_task.accountid,
salesforce_task.isclosed,
'1970-01-01'::date + (salesforce_task.createddate::bigint / 1000 * interval '1 second') as createddate,
'1970-01-01'::date + (salesforce_task.lastmodifieddate::bigint / 1000 * interval '1 second') as lastmodifieddate,
'1970-01-01'::date + (salesforce_task.systemmodstamp::bigint / 1000 * interval '1 second') as systemmodstamp,
salesforce_task.isarchived,
salesforce_task.calldisposition,
'1970-01-01'::date
+ (salesforce_task.datetimeclosed__c)::bigint / 1000 * interval '1 second' as date_time_closed_hcm__c,
'1970-01-01'::date
 + (salesforce_task.datetimeopened__c)::bigint / 1000 * interval '1 second' as date_time_opened_hcm__c,
DATE('1970-01-01'::date + salesforce_task.datetimeopened__c::bigint / 1000 * interval '1 second') as date_opened,
salesforce_task.department_orig__c,
salesforce_task.reasonforcall__c,
salesforce_task.upd_dt,
salesforce_task.recordtypeid,
salesforce_task.description2show__c,
salesforce_task.time_to_close_in_minutes__c,
salesforce_task.is_review_completed__c,
salesforce_task.source__c,
salesforce_task.description_to_sort__c,
salesforce_task.event_type__c,
salesforce_task.call_line__c,
salesforce_task.division__c,
salesforce_user.name,
salesforce_user.division,
salesforce_user.department,
salesforce_user.title,
salesforce_contact.id as contact_id,
salesforce_contact.name as contact_name,
salesforce_contact.mailingstreet as contact_mailing_street,
salesforce_contact.mailingcity as contact_mailing_city,
salesforce_contact.mailingstate as contact_mailing_state,
salesforce_contact.mailingstatecode as contact_mailing_state_code,
salesforce_contact.mailingpostalcode as contact_mailing_postal_code,
salesforce_contact.mailingcountry as contact_mailing_country,
salesforce_contact.mailingcountrycode as contact_mailing_country_code,
salesforce_contact.mailinglatitude as contact_mailing_latitude,
salesforce_contact.mailinglongitude as contact_mailing_longitude,
salesforce_contact.phone as contact_phone,
salesforce_contact.fax as contact_fax,
salesforce_contact.email as contact_email,
salesforce_contact.title as contact_title,
salesforce_contact.department__c as contact_department,
salesforce_contact.name_last_first__c as contact_name_last_first,
salesforce_contact.chop_employee__c as contact_chop_employee,
salesforce_contact.choplocation__c as contact_chop_location,
salesforce_contact.epic_id__c as contact_epic_id,
salesforce_account.name as contact_account_name
from  {{source('salesforce_ods', 'salesforce_task')}} as salesforce_task
inner join {{source('salesforce_ods', 'salesforce_user')}} as salesforce_user
on salesforce_task.ownerid = salesforce_user.id
left join  {{source('salesforce_ods', 'salesforce_contact')}} as salesforce_contact
on salesforce_task.whoid = salesforce_contact.id
left join  {{source('salesforce_ods', 'salesforce_account')}} as salesforce_account
on salesforce_contact.accountid = salesforce_account.id
left join  {{source('salesforce_hc_ods', 'salesforce_hc_task')}} as healthcloud_task
on salesforce_task.id = healthcloud_task.sales_cloud_taskid_hcm__c
where salesforce_user.crm_department__c = 'Access Center'
and healthcloud_task.id is null
