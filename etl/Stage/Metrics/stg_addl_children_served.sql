{{ config(meta = {
    'critical': true
}) }}

with chop_market_international as (
    select distinct
        stg_encounter_chop_market.pat_key,
        stg_encounter_chop_market.chop_market,
        stg_encounter_chop_market.region_category
    from
        {{ref('stg_encounter_chop_market')}} as stg_encounter_chop_market
        inner join {{ref ('stg_gps_healthcloud')}} as stg_gps_healthcloud
            on stg_gps_healthcloud.pat_key = stg_encounter_chop_market.pat_key
    where
        stg_gps_healthcloud.status in ('Closed Successful',
            'Closed Successful-Pending Log', 'Closed Successful-Pending LOG',
            'Current Patient', 'Closed Triage Only')
        and stg_encounter_chop_market.chop_market = 'international'
)

select
    stg_patient.mrn,
    stg_patient.dob,
    stg_patient.patient_name,
    cohort.pat_key,
    cohort.service_date,
    cohort.first_post_date,
    cohort.cost_center_name,
    cohort.cost_center_site,
    cohort.payor_name,
    cohort.care_setting,
    cohort.ip_ed_ind,
    cohort.source,
    cohort.revenue_location,
    cohort.department_name,
    cohort.department_center,
    initcap(patient_location.state) as state,
    patient_location.zip,
    coalesce(patient_location.zip,
    patient_location.zip_extend,
    patient_first_chop_location.zip,
    patient_first_chop_location.zip_extend) as zip_for_chop_market,
    case
        when chop_market_international.chop_market = 'international'
            then 'International'
            else coalesce(zip_market_mapping.chop_market, zip_market_mapping_chop.chop_market, 'Unknown')
        end as chop_market,
    case
        when chop_market_international.region_category = 'international'
            then 'International'
        when coalesce(zip_market_mapping.region_category, zip_market_mapping_chop.region_category) = ''
            then 'Unknown'
            else coalesce(zip_market_mapping.region_category, zip_market_mapping_chop.region_category, 'Unknown')
        end as region_category,
	case
		when lower(patient_name) like '%research%'
			then 1
			else 0
			end as research_ind,
	case
		when date(cohort.service_date) - date(stg_patient.dob) <= 30
			then 1
			else 0
		end as newborn_ind,
	case
		when cohort.service_date - lag(cohort.service_date)
		over(partition by stg_patient.mrn order by cohort.service_date) >= 1095
			or lag(cohort.service_date) over(partition by stg_patient.mrn order by cohort.service_date) is null
			then 1
			else 0
		end as new_enterprise_patient_ind,
    case
        when cohort.service_date - lag(cohort.service_date)
        over(partition by cohort.care_setting || stg_patient.mrn order by cohort.service_date) >= 1095
        -- for each patient in each care setting, see if the intervals between service dates >= 1095 days
            or lag(cohort.service_date)
                over(partition by cohort.care_setting || stg_patient.mrn order by cohort.service_date) is null
        -- if patient has no previous service_date (has only had 1 service date with CHOP)
            then 1
            else 0
        end as new_care_setting_patient_ind,
	cohort.row_num
from
{{ref('stg_charges_row_num')}} as cohort
inner join {{ref('stg_patient')}} as stg_patient
        on cohort.pat_key = stg_patient.pat_key
left join
    {{ref('stg_addl_children_location')}} as patient_location
    on cohort.pat_key = patient_location.pat_key
    and cohort.service_date = patient_location.service_date
    and patient_location.line_most_recent_address = 1
left join
    {{ref('stg_addl_children_location')}} as patient_first_chop_location
    on cohort.pat_key = patient_first_chop_location.pat_key
    and patient_first_chop_location.line_most_recent_chop_address = 1
left join
    {{ref('stg_dim_zip_market_mapping')}} as zip_market_mapping
    on zip_market_mapping.zip = coalesce(patient_location.zip, patient_location.zip_extend)
left join
    {{ref('stg_dim_zip_market_mapping')}} as zip_market_mapping_chop
    on zip_market_mapping_chop.zip = coalesce(patient_first_chop_location.zip,
    patient_first_chop_location.zip_extend)
left join chop_market_international
    on chop_market_international.pat_key = cohort.pat_key
where
    cohort.service_date >= to_date('2015-01-01', 'yyyy-mm-dd')
    and cohort.row_num = 1
