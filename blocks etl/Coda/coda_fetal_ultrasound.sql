with fetal_ultrasound as (
	select
		cast(usfetalcoda.accession as int) as accession_id,
		usfetalcoda.proceduredesclist,
		usfetalcoda.lastsigndate,
		usfetalcoda.accountid,
		usfetalcoda.mrn,
		usfetalcoda.completedate,
		usfetalcoda.lastname,
		usfetalcoda.firstname,
		case when usfetalcoda.average_ultrasound_age = '*NO DATA*' then null
			else average_ultrasound_age end as average_ultrasound_age,
		replace(regexp_extract(usfetalcoda.abdomen_circumference, '[\d\.]+\s?cm'), 'cm', '') as abdomen_circumference,
		replace(regexp_extract(usfetalcoda.amniotic_fluid_index, '[\d\.]+\s?cm'), 'cm', '') as amniotic_fluid_index,
		trim(replace(regexp_extract(usfetalcoda.average_ultrasound_age, '[\d]+\s?weeks'), 'weeks', ''))
			as average_weeks,
		trim(replace(regexp_extract(usfetalcoda.average_ultrasound_age, '[\d]+\s?days'), 'days', '')) as average_days,
		replace(regexp_extract(usfetalcoda.biparietal_diameter, '[\d\.]+\s?cm'), 'cm', '') as biparietal_diameter,
		replace(regexp_extract(usfetalcoda.cervical_length, '[\d\.]+\s?cm'), 'cm', '') as cervical_length,
		case
			when lower(usfetalcoda.cervical_method) like '%dominal%'
				or lower(usfetalcoda.cervical_method) like '%abominally%'
					then 'transabdominal'
			when lower(usfetalcoda.cervical_method) like '%vaginal%'
				then  'transvaginal'
			else null
		end as cervical_method,
		case
			when lower(usfetalcoda.cervical_method) like '%dominal%'
				or lower(usfetalcoda.cervical_method) like '%abominally%'
					then replace(regexp_extract(usfetalcoda.cervical_length, '[\d\.]+\s?cm'), 'cm', '')
		end as  cervical_transabdominal_length,
		case
			when lower(usfetalcoda.cervical_method) like '%vaginal%'
				then  replace(regexp_extract(usfetalcoda.cervical_length, '[\d\.]+\s?cm'), 'cm', '')
			else null
		end as cervical_transvaginal_length,
		duct_venosus_mapping.ductus_venosus_map as ductus_venosus,
		placenta_location_mapping.placenta_location_map as placenta_location,
		umbilical_flow_mapping.umbilical_flow_map as umbilical_vein_flow,
		trim(replace(regexp_extract(usfetalcoda.estimated_fetal_weight, '[\d\.]+\s?g'), 'g', ''))
			as estimated_fetal_weight,
		replace(regexp_extract(usfetalcoda.fetal_head_circumference, '[\d\.]+\s?cm'), 'cm', '')
			as fetal_head_circumference,
		replace(regexp_extract(usfetalcoda.deepest_vertical_pocket, '[\d\.]+\s?cm'), 'cm', '')
			as deepest_vertical_pocket,
		replace(regexp_extract(usfetalcoda.femur_length, '[\d\.]+\s?cm'), 'cm', '') as femur_length,
		replace(regexp_extract(usfetalcoda.fetal_heart_rate, '[\d\.]+\s?bpm'), 'bpm', '') as fetal_heart_rate,
		case when usfetalcoda.hc_ac = '*NO DATA*' then null
			else replace(usfetalcoda.hc_ac, ',', '') end as hc_ac,
		case when usfetalcoda.ct_ratio = '*NO DATA*' then null
			else replace(usfetalcoda.ct_ratio, ',', '') end as ct_ratio,
		case when usfetalcoda.umbilica_artery_sd_ratio = '*NO DATA*' then null
			else replace(usfetalcoda.umbilica_artery_sd_ratio, ';', '') end as umbilica_artery_sd_ratio,
		usfetalcoda.upd_dt
	from
		{{ source('powerscribe_ods', 'powerscribe_usfetalcoda') }} as usfetalcoda
        left join {{ref('lookup_coda_us_duct_venosus_mapping')}} as duct_venosus_mapping
            on duct_venosus_mapping.duct_venosus_text = usfetalcoda.ductus_venosus
        left join {{ref('lookup_coda_us_placenta_location_mapping')}} as placenta_location_mapping
            on placenta_location_mapping.placenta_location_text = usfetalcoda.placenta_location
        left join {{ref('lookup_coda_us_umbilical_flow_mapping')}} as umbilical_flow_mapping
            on umbilical_flow_mapping.umbilical_flow_text = usfetalcoda.umbilical_vein_flow
)
select
	max(fetal_ultrasound.accession_id) as accession_id,
	stg_patient.pat_key,
	stg_patient.patient_name,
	stg_patient.dob,
	fetal_ultrasound.mrn,
	fetal_ultrasound.completedate,
	fetal_ultrasound.lastsigndate,
	fetal_ultrasound.accountid,
	fetal_ultrasound.lastname as physician_lastname,
	fetal_ultrasound.firstname as physician_firstname,
	group_concat(proceduredesclist, ':') as proceduredesclist,
	max(fetal_ultrasound.average_ultrasound_age) as average_ultrasound_age,
	max(fetal_ultrasound.abdomen_circumference) as abdomen_circumference,
	max(fetal_ultrasound.amniotic_fluid_index) as amniotic_fluid_index,
	cast(max(fetal_ultrasound.average_weeks) as int) as average_weeks,
	cast(max(fetal_ultrasound.average_days) as int) as average_days,
	max(fetal_ultrasound.biparietal_diameter) as biparietal_diameter,
	max(fetal_ultrasound.cervical_length) as cervical_length,
	max(fetal_ultrasound.ductus_venosus) as ductus_venosus,
	max(fetal_ultrasound.placenta_location) as placenta_location,
	max(fetal_ultrasound.umbilical_vein_flow) as umbilical_vein_flow,
	max(fetal_ultrasound.cervical_method) as cervical_method,
	max(cervical_transabdominal_length) as cervical_transabdominal_length,
	max(cervical_transvaginal_length) as cervical_transvaginal_length,
	cast(max(fetal_ultrasound.estimated_fetal_weight) as int) as estimated_fetal_weight,
	max(fetal_ultrasound.fetal_head_circumference) as fetal_head_circumference,
	max(fetal_ultrasound.deepest_vertical_pocket) as deepest_vertical_pocket,
	max(fetal_ultrasound.femur_length) as femur_length,
	cast(max(fetal_ultrasound.fetal_heart_rate) as int) as fetal_heart_rate,
	max(fetal_ultrasound.hc_ac) as hc_ac,
	max(fetal_ultrasound.ct_ratio) as ct_ratio,
	max(fetal_ultrasound.umbilica_artery_sd_ratio) as umbilica_artery_sd_ratio,
	max(fetal_ultrasound.upd_dt) as upd_dt
from
	fetal_ultrasound as fetal_ultrasound
	inner join {{ref('stg_patient')}} as stg_patient
        on fetal_ultrasound.mrn = stg_patient.mrn
group by
	stg_patient.pat_key,
	stg_patient.patient_name,
	stg_patient.dob,
	fetal_ultrasound.mrn,
	fetal_ultrasound.completedate,
	fetal_ultrasound.lastsigndate,
	fetal_ultrasound.accountid,
	fetal_ultrasound.lastname,
	fetal_ultrasound.firstname
