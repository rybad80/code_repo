select
    id,
	case_name_hcm__c,
	date(strleft(sales_cloud_created_date_hcm__c,10)) as referral_date,
	cg_mrn__c,
	oncology_team_hcm__c,
	status,
    case
        when lower(status) like '%completed%'
            then 1
            else 0
        end as converted_ind,
	referring_organization__c,
    'OP Cancer Center' as drill_down,
    {{
    dbt_utils.surrogate_key([
        'id',
        'drill_down'
        ])
    }} as primary_key
from
    {{source('salesforce_hc_ods', 'salesforce_hc_case')}}
where
	lower(source_hcm__c) = 'oncology'
	and recordtypeid = '0123i0000010DAnAAM'
