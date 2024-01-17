with all_metrics as (
	select
		'Fill Rate' as metric_name,
		case
			when specialty_care_slot_ind = 1 --specialty care grouper
				and lower(specialty_name) in (
					'physical therapy', 'speech', 'audiology', 'occupational therapy', 'clinical nutrition'
				)
				then 'Ancillary Services'
			when specialty_care_slot_ind = 1
				and physician_app_psych_ind = 1
				and lower(specialty_name) not in (
					'cardiovascular surgery', 'family planning', 'clinical nutrition', 'multidisciplinary', 'obstetrics'
				)
				then 'Specialty Care'
			when primary_care_slot_ind = 1
				then 'Primary Care'
				end as	care_setting,
		scheduling_provider_slot_status.specialty_name,
		scheduling_provider_slot_status.department_name,
		scheduling_provider_slot_status.department_id,
		{{
		dbt_utils.surrogate_key([
			'slot_start_time',
			'prov_key',
			'dept_key'
			])
		}} as primary_key, --used to check granularity of data
		scheduling_provider_slot_status.encounter_date as index_date,
		scheduled_ind as numerator,
		available_ind as denominator,
		null as group_by
	from
		{{ref('scheduling_provider_slot_status')}} as scheduling_provider_slot_status
	where
		fill_rate_incl_ind = 1
		and encounter_date < current_date
		and ((specialty_care_slot_ind = 1 --specialty care grouper
			and lower(specialty_name) in (
				'physical therapy', 'speech', 'audiology', 'occupational therapy', 'clinical nutrition'
			)
			)
		or (specialty_care_slot_ind = 1
			and physician_app_psych_ind = 1
			and lower(specialty_name) not in (
				'cardiovascular surgery', 'family planning', 'clinical nutrition', 'multidisciplinary', 'obstetrics'
			)
			)
		or primary_care_slot_ind = 1)
	union all
	select
		'New Patient Lag Time (Median)' as metric_name,
		case
			when intended_use_id = 1009
				and physician_app_psych_visit_ind = 1
				and lower(specialty_name) not in (
					'cardiovascular surgery', 'obstetrics', 'multidisciplinary', 'gi/nutrition', 'family planning'
				)
				then 'Specialty Care'
			when intended_use_id = 1009
				and lower(specialty_name) in (
					'physical therapy', 'speech', 'audiology', 'occupational therapy', 'clinical nutrition'
				)
				then 'Ancillary Services'
			when intended_use_id = 1013
				then 'Primary Care'
				end as care_setting,
		specialty_name,
		department_name,
		department_id,
		{{
		dbt_utils.surrogate_key([
			'visit_key'
			])
		}} as primary_key,--used to check granularity of data
		appointment_made_date as index_date,
		npv_appointment_lag_days as numerator,
		null as denominator,
		null as group_by
	from
		{{ref('stg_encounter_outpatient')}}
	where
		npv_lag_incl_ind = 1
		and ((intended_use_id = 1009
			and physician_app_psych_visit_ind = 1
			and lower(specialty_name) not in (
				'cardiovascular surgery', 'obstetrics', 'multidisciplinary', 'gi/nutrition', 'family planning'
			)
			)
		or (intended_use_id = 1009
			and lower(specialty_name) in (
				'physical therapy', 'speech', 'audiology', 'occupational therapy', 'clinical nutrition'
			)
			)
		or intended_use_id = 1013)
	union all
	select
		'Ease of Scheduling Top Box Score' as metric_name,
		survey_line_name as care_setting,
		survey_line_name as specialty_name,
		department_name,
		department_id,
		{{
		dbt_utils.surrogate_key([
			'survey_key'
			])
		}} as primary_key,
		visit_date as index_date,
		tbs_ind as numerator,
		survey_key as denominator,
		null as group_by
	from
		{{ref('pfex_all')}}
	where
		lower(survey_line_name) in ('primary care', 'specialty care')
		and question_id = 'A1' --ease of scheduling question
	union all
	select
		'Online Scheduling' as metric_name, 
		case
			when stg_department_all.intended_use_id = 1009
				and physician_app_psych_visit_ind = 1
				and lower(stg_department_all.specialty_name) not in (
					'cardiovascular surgery',
					'obstetrics',
					'multidisciplinary',
					'gi/nutrition',
					'family planning',
					'clinical nutrition'
				)
				then 'Specialty Care'
			when stg_department_all.intended_use_id = 1009
				and lower(stg_department_all.specialty_name) in (
					'physical therapy', 'speech', 'audiology', 'occupational therapy', 'clinical nutrition'
				)
				then 'Ancillary Services'
			when stg_department_all.intended_use_id = 1013
				then 'Primary Care'
				end as care_setting,
		stg_department_all.specialty_name, 
		stg_department_all.department_name,
		stg_encounter.department_id,
		{{
		dbt_utils.surrogate_key([
			'stg_encounter.visit_key'
			])
		}} as primary_key,
		stg_encounter.original_appointment_made_date as index_date,
		case
			when care_setting = 'Specialty Care'
				then stg_encounter_outpatient.phys_app_psych_online_scheduled_ind
			else stg_encounter.online_scheduled_ind
		end as numerator,
		stg_encounter.visit_key as denominator,
		null as group_by
	from
		{{ref('stg_encounter')}} as stg_encounter
	left join 
		{{ref('stg_encounter_outpatient')}} as stg_encounter_outpatient
			on stg_encounter_outpatient.visit_key = stg_encounter.visit_key
	left join
		{{ref('stg_department_all')}} as stg_department_all
		on stg_encounter.department_id = stg_department_all.department_id
	where
		stg_encounter.walkin_ind = 0 
		and stg_encounter.original_appointment_made_date is not null
		and ((stg_department_all.intended_use_id = 1009 
			and physician_app_psych_visit_ind = 1 
			and lower(stg_department_all.specialty_name)
			not in ('cardiovascular surgery', 'obstetrics', 'multidisciplinary', 'gi/nutrition', 'family planning'))
		or (stg_department_all.intended_use_id = 1009 
			and lower(stg_department_all.specialty_name)
			in ('physical therapy', 'speech', 'audiology', 'occupational therapy', 'clinical nutrition'))
		or stg_department_all.intended_use_id = 1013)
	union all
	select
		'Third Next Available' as metric_name,
		care_setting,
		specialty_name,
		department_name,
		department_id,
		{{
		dbt_utils.surrogate_key([
			'specialty_name',
			'department_name',
			'index_date',
			'group_by'
			])
		}} as primary_key,
		index_date,
		days_to_slot as numerator,
		1 as denominator,
		group_by
	from
		{{ref('stg_third_next_available_espd')}}
)
select 
	all_metrics.metric_name,
	all_metrics.care_setting,
	all_metrics.specialty_name,
	all_metrics.department_name,
	coalesce(stg_department_all.revenue_location_group, 'Other') as revenue_location_group,
	all_metrics.primary_key,
	all_metrics.index_date,
	all_metrics.numerator,
	all_metrics.denominator,
	all_metrics.group_by
from 
    all_metrics
left join 
	{{ref('stg_department_all')}} as stg_department_all
		on all_metrics.department_id = stg_department_all.department_id