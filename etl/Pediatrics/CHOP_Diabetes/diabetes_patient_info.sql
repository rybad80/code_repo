with listed_races as ( --One patient may have multiple races selected
select
	patient_race_ethnicity.pat_key,
	race.dict_nm
from
    {{source('cdw', 'patient_race_ethnicity')}} as patient_race_ethnicity
	left join {{source('cdw', 'cdw_dictionary')}} as race
        on race.dict_key = patient_race_ethnicity.dict_race_ethnic_key
			and patient_race_ethnicity.race_ind = 1	--The value 1 indicates that the record is a patient's race
where
	race.dict_nm is not null
group by
	patient_race_ethnicity.pat_key,
	race.dict_nm
)
select
	diabetes_patient_all.patient_key,
	diabetes_patient_all.mrn,
	diabetes_patient_all.patient_name,
	diabetes_patient_all.dob,
	patient_all.sex,
	patient_all.current_age,
	dim_provider.full_name as pcp_nm, 	--patient's current Primary Care Provider
	listed_races.dict_nm as race, 	--race(s) per patient
	case when patient_all.race = 'Multi-Racial' then 1 else 0 end as multi_racial_ind,
	patient_all.ethnicity,
	patient_all.race_ethnicity,
	patient_all.mailing_city,
	patient_all.mailing_state,
	patient_all.mailing_zip,
	patient_all.county,
	patient_all.preferred_language,
	equity_coi2.opportunity_lvl_coi_natl_norm,
	equity_coi2.opportunity_lvl_education_domain_natl_norm,
	equity_coi2.opportunity_lvl_healthenv_domain_natl_norm,
	equity_coi2.opportunity_lvl_socioeconomic_domain_natl_norm,
	equity_coi2.opportunity_lvl_coi_state_norm,
	equity_coi2.opportunity_lvl_education_domain_state_norm,
	equity_coi2.opportunity_lvl_healthenv_domain_state_norm,
	equity_coi2.opportunity_lvl_socioeconomic_domain_state_norm,
	equity_coi2.opportunity_score_coi_natl_norm,
	equity_coi2.opportunity_score_coi_state_norm,
	patient.interpreter_needed_ind
from
	{{ref('diabetes_patient_all')}} as diabetes_patient_all
	inner join {{ref('patient_all')}} as patient_all 	on patient_all.pat_key = diabetes_patient_all.pat_key
										and current_record_ind = 1 --the most recent record
	inner join {{source('cdw', 'patient')}} as patient 	on patient.pat_key = diabetes_patient_all.pat_key
	--temp join to cdw.provider until prov_key fully swapped out
	inner join {{source('cdw', 'provider')}} as provider on provider.prov_key = patient.prov_key
	inner join {{ ref('dim_provider') }} as dim_provider on dim_provider.prov_id = provider.prov_id
	left join listed_races on listed_races.pat_key = patient.pat_key
	left join {{ source('cdw', 'patient_geographical_spatial_info') }} as patient_geographical_spatial_info
        on patient_all.pat_key = patient_geographical_spatial_info.pat_key
			and patient_geographical_spatial_info.seq_num = 0 -- only current address
            -- only records where match was based on address
            and patient_geographical_spatial_info.locator_nm in ('ADDRESS_POINTS', 'STREET_ADDRESS')
            -- only records where service is confident in match quality
            and patient_geographical_spatial_info.accuracy_score >= 85
	-- pull most recent year of COI data
	left join
        {{ ref('equity_coi2') }} as equity_coi2
        on patient_geographical_spatial_info.census_tract_key = equity_coi2.census_tract_key
            and equity_coi2.observation_year = 2015
group by
	diabetes_patient_all.patient_key,
	diabetes_patient_all.mrn,
	diabetes_patient_all.patient_name,
	diabetes_patient_all.dob,
	patient_all.sex,
	patient_all.current_age,
	pcp_nm,
	listed_races.dict_nm,
	multi_racial_ind,
	patient_all.ethnicity,
	patient_all.race_ethnicity,
	patient_all.mailing_city,
	patient_all.mailing_state,
	patient_all.mailing_zip,
	patient_all.county,
	patient_all.preferred_language,
	equity_coi2.opportunity_lvl_coi_natl_norm,
	equity_coi2.opportunity_lvl_education_domain_natl_norm,
	equity_coi2.opportunity_lvl_healthenv_domain_natl_norm,
	equity_coi2.opportunity_lvl_socioeconomic_domain_natl_norm,
	equity_coi2.opportunity_lvl_coi_state_norm,
	equity_coi2.opportunity_lvl_education_domain_state_norm,
	equity_coi2.opportunity_lvl_healthenv_domain_state_norm,
	equity_coi2.opportunity_lvl_socioeconomic_domain_state_norm,
	equity_coi2.opportunity_score_coi_natl_norm,
	equity_coi2.opportunity_score_coi_state_norm,
	patient.interpreter_needed_ind
