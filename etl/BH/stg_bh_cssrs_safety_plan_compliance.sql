select
	bh_screenings_summary.visit_key,
	bh_screenings_summary.pat_key,
	bh_screenings_summary.cssrs_entered_date,
	stg_encounter.mrn,
	initcap(stg_encounter.department_name) as department_name,
	stg_encounter_ed.initial_ed_department_center_abbr,
	initcap(stg_encounter.patient_class) as patient_class,
	initcap(stg_encounter_payor.payor_group) as payor_group,
	round(stg_encounter.age_years) as age_years,
    initcap(dim_provider.full_name) as provider_name,
	date(stg_encounter.hospital_admit_date) as hospital_admit_date,
	date(stg_encounter.hospital_discharge_date) as hospital_discharge_date,
    case when round(stg_encounter.age_years) < 7 then '0-6 y/o'
        when  round(stg_encounter.age_years) < 13 then '7-12 y/o'
        when  round(stg_encounter.age_years) < 19 then '13-18 y/o'
        when round(stg_encounter.age_years) >= 19 then '19+ y/o'
        else null
    end as age_groups,
    case when race in ('Native Hawaiian or Other Pacific Islander', 'Indian', 'American Indian or Alaska Native',
                        null, 'Refused') then 'Other'
        when race = 'Black or African American' then 'African American' else race
    end as race_groups,
    stg_patient.ethnicity,
    stg_patient.sex,
    social_vulnerability_index.overall_category as svi_category
from {{ref('bh_screenings_summary')}} as bh_screenings_summary
left join {{ref('stg_encounter')}} as stg_encounter
    on stg_encounter.visit_key = bh_screenings_summary.visit_key
left join {{ref('dim_provider')}} as dim_provider
    on dim_provider.provider_key = stg_encounter.provider_key
left join {{ref('stg_encounter_ed')}} as stg_encounter_ed
    on stg_encounter_ed.visit_key = bh_screenings_summary.visit_key
left join {{ref('encounter_inpatient')}} as encounter_inpatient
    on encounter_inpatient.visit_key = bh_screenings_summary.visit_key
left join {{ref('stg_encounter_payor')}} as stg_encounter_payor
    on stg_encounter.visit_key = stg_encounter_payor.visit_key
left join {{ref('transport_encounter_all')}} as transport_encounter_all
    on bh_screenings_summary.visit_key = transport_encounter_all.admit_visit_key
left join {{ref('stg_patient')}} as stg_patient
    on bh_screenings_summary.pat_key = stg_patient.pat_key
left join {{source('ods', 'patient_geospatial_temp')}} as patient_geospatial_temp
    on stg_encounter.pat_key = patient_geospatial_temp.pat_key
left join {{source('cdc_ods', 'social_vulnerability_index')}} as social_vulnerability_index
    on patient_geospatial_temp.census_tract_fips = social_vulnerability_index.fips
where date(stg_encounter.hospital_discharge_date) >= '2019-07-01'
    and (encounter_inpatient.discharge_disposition is null
        or encounter_inpatient.discharge_disposition = 'Discharged (routine)')
    and stg_encounter_ed.ed_arrival_date is not null
    and (bh_screenings_summary.cssrs_qi_positive_ind = 1
        and bh_screenings_summary.cssrs_declined_ind = 0)
    and case when transport_encounter_all.transport_type = 'Outbound'
        and transport_encounter_all.final_status = 'completed' then 1 end is null
