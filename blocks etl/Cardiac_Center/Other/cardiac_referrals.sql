select
     cases.id as case_id,
     account.name as account_name,
     cases.subject as case_subject,
     cases.cg_mrn__c as mrn,
     cases.cg_date_of_birth__c as dob,
     account.personmailingstreet as street_address,
     account.personmailingcity as city,
     account.personmailingstate as state,
     account.personmailingpostalcode as zipcode,
     account.personmailingcountry as county,
     account.phone as phone_number,
     account.personemail as email_address,
     account.family_s_preferred_language__c as preferred_language,
     date(substring(cases.createddate, 1, 10)) as create_date,
     cases.isclosed as is_closed,
     leadsource_hcm__c as lead_source,
     services__c as cardiac_services,
     cases.request_type__c as request_type,
     cases.status as case_status,
     substatus_hcm__c as case_substatus,
     referring_provider.name as referring_provider,
     referring_organization.name as referring_organization,
     sales_could_id_hcm__c as opportunity_id
from
  {{source('salesforce_hc_ods', 'salesforce_hc_case')}} as cases
  inner join {{source('salesforce_hc_ods', 'salesforce_hc_account')}} as account
     on cases.accountid = account.id
  left join {{source('salesforce_hc_ods', 'salesforce_hc_contact')}} as referring_provider
     on cases.referring_provider_hcm__c = referring_provider.id
  left join {{source('salesforce_hc_ods', 'salesforce_hc_account')}} as referring_organization
     on cases.referring_organization__c = referring_organization.id
  left join {{source('salesforce_hc_ods', 'salesforce_hc_opportunity')}} as opportunity
     on cases.sales_could_id_hcm__c = opportunity.id
where
     ((sales_could_id_hcm__c is not null and date(substring(cases.createddate, 1, 10)) < '2022-12-07')
     or (sales_could_id_hcm__c is null and date(substring(cases.createddate, 1, 10)) >= '2022-12-07'))
     and coalesce(substatus_hcm__c, '') != 'Referred for Second Opinion'
     and cases.recordtypeid = '012Do000000TeEcIAK'
