select
	smart_data_element_all.pat_key,
	smart_data_element_all.visit_key,
	date(smart_data_element_all.entered_date) as recorded_date,
	case
		when smart_data_element_all.concept_id = 'CHOP#6984' then 'diab_type'
		when smart_data_element_all.concept_id = 'CHOP#6983' then 'team'
		when smart_data_element_all.concept_id = 'CHOP#7527' then 'ed visits'
		when smart_data_element_all.concept_id = 'CHOP#7530' then 'ip visits'
		when smart_data_element_all.concept_id = 'CHOP#6985' then 'dx date'
		when smart_data_element_all.concept_id = 'CHOP#7245' then 'diab_regimen'
        --Primary Diabetes Provider last name
		when smart_data_element_all.concept_id = 'CHOP#6982' then 'np'
	end as fs_type,
	--add prov_key to link ad_login in diabetes_patient_all and setup conditional access in Qlik Sense for NP:
	case
        when fs_type = 'np' then dim_provider.provider_key
    end as np_prov_key,
	case
		when fs_type = 'np' then dim_provider.last_name
		when fs_type = 'team' and smart_data_element_all.element_value = 'Philly - Monday Meerkats'
			then 'Philly- Monday Meerkats'
		when fs_type = 'team' and smart_data_element_all.element_value = 'LGH - Llama'
            then 'LGH - Llamas'
        else smart_data_element_all.element_value
    end as meas_val
from
	{{ ref('smart_data_element_all') }} as smart_data_element_all
	left join {{ ref('dim_provider') }} as dim_provider
        on dim_provider.prov_id = smart_data_element_all.element_value
            --9403 Primary Diabetes Provider
            and smart_data_element_all.concept_id = 'CHOP#6982'
where
	smart_data_element_all.concept_id in (
        'CHOP#6984', --7261 --Type of Diabetes
        'CHOP#6983', --10060215	--Diabetes Team
        'CHOP#7527', --15773 --CHOP R ENDO ED DKA # OF EPISODES
        'CHOP#7530', --15778	--CHOP R ENDO INPATIENT DKA # OF EPISODES
        'CHOP#6985', --7251 --Date of Diagnosis
        'CHOP#7245', --10060217	--Diabetes Regimen
        'CHOP#6982' --9403	--Primary Diabetes Provider
	)
    --some SDE doc didn't include valid encounter_date, use ENTERED_DATE instead:
	and date(smart_data_element_all.entered_date) <= current_date
    --	diabetes samrtform has launched since 2023, identify active patients since 2023:
	and date(smart_data_element_all.entered_date) >= '2022-12-31'
group by
	smart_data_element_all.pat_key,
	smart_data_element_all.visit_key,
	date(smart_data_element_all.entered_date),
	fs_type,
	np_prov_key,
	meas_val
union all
/*A1c flowsheet is active in both old and new workflow*/
select
	flowsheet_all.pat_key,
	flowsheet_all.visit_key,
	date(flowsheet_all.recorded_date) as recorded_date,
	'a1c' as fs_type,
	null as np_prov_key,
	flowsheet_all.meas_val
from
	{{ ref('flowsheet_all') }} as flowsheet_all
where
    --	most recent a1c icr flowsheets is used by both old and new workflow:
	flowsheet_all.flowsheet_id = 10060217
	and flowsheet_all.recorded_date <= current_date
    --last data reload date of diabetes_icr_active_flowsheets_historical
	and flowsheet_all.recorded_date >= '2023-02-09'
group by
	flowsheet_all.pat_key,
	flowsheet_all.visit_key,
	date(flowsheet_all.recorded_date),
	fs_type, --noqa: L028
	np_prov_key, --noqa: L028
	flowsheet_all.meas_val
union all
/*old workflow retired: all icr flowsheets launched since 2012, identify active patients between cy2011 to cy2022*/
select
	diabetes_icr_active_flowsheets_historical.pat_key,
	diabetes_icr_active_flowsheets_historical.visit_key,
	date(diabetes_icr_active_flowsheets_historical.recorded_date) as recorded_date,
	diabetes_icr_active_flowsheets_historical.fs_type,
	null as np_prov_key,
	diabetes_icr_active_flowsheets_historical.meas_val
from
	{{ ref('diabetes_icr_active_flowsheets_historical') }} as diabetes_icr_active_flowsheets_historical
