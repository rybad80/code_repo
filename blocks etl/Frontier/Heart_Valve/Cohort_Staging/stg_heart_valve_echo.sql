with
echo_registry as (
	select
		cardiac_echo.visit_key,
		cardiac_valve_center.mrn,
		stg_patient_ods.patient_name,
		cardiac_echo.study_date as encounter_date,
		cardiac_echo.echo_type,
		replace(cardiac_echo_details.attending, ' MD', '') as provider_name
	from {{ ref('cardiac_valve_center') }} as cardiac_valve_center
		left join {{ ref('stg_patient_ods') }} as stg_patient_ods
			on cardiac_valve_center.mrn = stg_patient_ods.mrn
		inner join {{ ref('cardiac_echo') }} as cardiac_echo
			on cardiac_valve_center.mrn = cardiac_echo.mrn
		inner join {{ ref('cardiac_echo_details') }} as cardiac_echo_details
			on cardiac_echo.cardiac_study_id = cardiac_echo_details.cardiac_study_id
	where
		year(add_months(cardiac_echo.study_date, 6)) > '2020'
		and regexp_like(lower(cardiac_echo_details.attending),
			'rogers, l.*|
			|quartermain, m.*|
			|jolley, m.*|
			|savla, j.*|
			|padiyath, a.*|
			|williams, t.*'
		)
		and cardiac_echo.visit_key != '0'
	group by
		cardiac_echo.visit_key,
		cardiac_valve_center.mrn,
		stg_patient_ods.patient_name,
		cardiac_echo.study_date,
		cardiac_echo.echo_type,
		cardiac_echo_details.attending
),
echo_non_registry as (
	select
		procedure_order_all.visit_key,
		procedure_order_all.mrn,
		stg_encounter.patient_name,
		procedure_order_all.encounter_date,
		procedure_order_all.procedure_name as echo_type,
		coalesce(
			provider_encounter_care_team.provider_care_team_name,
			stg_encounter.provider_name)
		as provider_name
		from {{ ref('procedure_order_all') }} as procedure_order_all
		left join {{ ref('stg_encounter') }} as stg_encounter
			on procedure_order_all.visit_key = stg_encounter.visit_key
		left join {{ ref('provider_encounter_care_team') }} as provider_encounter_care_team
			on procedure_order_all.visit_key = provider_encounter_care_team.visit_key
	where
		lower(cpt_code) in (
						'500card74',	--ip transesophageal echo 2d/3d valve anesthesia sedated
						'500card72',	--ip transthoracic 2d/3d valve w anes sedated
						'93004.006',	--op transesophageal echo 2d/3d valve anesthesia sedated
						'93004.004'	 --op transthoracic echo 2d/3d valve anesthesia sedated
						)
		and lower(appointment_status) != 'no show'
		and lower(stg_encounter.encounter_type) != 'orders only'
	group by
		procedure_order_all.visit_key,
		procedure_order_all.mrn,
		stg_encounter.patient_name,
		procedure_order_all.encounter_date,
		procedure_order_all.procedure_name,
        provider_encounter_care_team.provider_care_team_name,
        stg_encounter.provider_name
)
select
	visit_key,
	mrn,
	patient_name,
	encounter_date,
	echo_type,
	provider_name
from echo_registry
union all
select
	visit_key,
	mrn,
	patient_name,
	encounter_date,
	echo_type,
	provider_name
from echo_non_registry
